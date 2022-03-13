import asyncio
import hashlib
import random
import threading
from abc import ABC, abstractmethod
from collections import OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor
from queue import PriorityQueue

from typing import Callable, List, Optional

import psutil
import ray
from ray.actor import ActorHandle

from ralf.policies import load_shedding_policy, processing_policy
from ralf.record import Record, Schema, TableState
from ralf.state import Connector

DEFAULT_STATE_CACHE_SIZE: int = 0

# This should represent a pool of sharded operators.
class ActorPool:
    def __init__(self, handles: List[ActorHandle]):
        self.handles = handles
        self._lazy = ray.get(handles[0].is_lazy.remote())

    @classmethod
    def make_replicas(cls, num_replicas, actor_class, *init_args, **init_kwargs):
        assert num_replicas > 0

        handles = [
            actor_class.options(max_concurrency=int(1e9)).remote(
                *init_args, **init_kwargs
            )
            for _ in range(num_replicas)
        ]
        for i, handle in enumerate(handles):
            handle.set_current_actor_handle.remote(handle)
            handle.set_shard_idx.remote(i)
        return cls(handles)

    def hash_key(self, key: str) -> int:
        # TODO: Figure out hashing for non-string keys
        hash_val = hashlib.sha1(key.encode("utf-8")).hexdigest()
        return int(hash_val, 16)

    def choose_actor(self, key) -> ActorHandle:
        return self.handles[self.hash_key(key) % len(self.handles)]

    # TODO: remove?
    def get(self, key):
        res = ray.get(self.choose_actor(key).get.remote(key))
        return res

    def get_async(self, key) -> ray.ObjectRef:
        return self.choose_actor(key).get.remote(key)

    def get_all_async(self) -> List[ray.ObjectID]:
        return [handle.get_all.remote() for handle in self.handles]

    def broadcast(self, attr, *args):
        # TODO: fix having to wait
        outputs = []
        for handle in self.handles:
            outputs.append(getattr(handle, attr).remote(*args))
        return outputs

    def is_lazy(self) -> bool:
        return self._lazy


class Event:
    """An event corresponding to a record that is processed by the operator.

    Orders events according to `processing_policy` if one is provided,
    otherwise orders events based on record processing time.
    """

    def __init__(
        self,
        work: Callable[[], None],
        record: Record,
        processing_policy: Callable[["Event", "Event"], bool],
    ):
        self._work = work
        self.record = record
        self._processing_policy = processing_policy

    def __lt__(self, other) -> bool:
        return self._processing_policy(self.record, other.record)

    def __eq__(self, other) -> bool:
        return self._time == other._time

    def process(self):
        self._work()


class Operator(ABC):
    """Abstract Operator class.

    Transforms data from parent tables and stores results in an output table.
    Operators can compute lazily or eagerly, and manage queries to the output table.
    Computation can be multithreaded, as well as sharded by key across multiple
        processes.

    Args:
        schema: schema of the output table.
        cache_size: number of records stored in memory for the output table.
        lazy: whether records are produced lazily (on request) or eagerly.
        num_worker_threads: number of concurrent threads with which recrods are
            produced.
        procesing_policy: a function that returns true if the first record should be
            processed before the second. By default, processes records in order of
            processing time.
        load_shedding_policy: decides whether to process the candidate record given that
            the current record is already present in the output table.
    """

    def __init__(
        self,
        schema: Schema,
        connector: Connector,
        cache_size=DEFAULT_STATE_CACHE_SIZE,
        lazy: bool = False,
        num_worker_threads: int = 4,
        processing_policy: Callable[[Record, Record], bool] = processing_policy.fifo,
        load_shedding_policy: Callable[
            [Record, Record], bool
        ] = load_shedding_policy.always_process,
    ):

        # Mained output table state
        self._table = TableState(schema, connector)
        self._cache_size = cache_size
        self._lru = OrderedDict()
        self._lazy = lazy
        self._events = defaultdict(PriorityQueue)
        self._empty_queue_event = threading.Event()
        self._running = True
        self._thread_pool = ThreadPoolExecutor(num_worker_threads)
        self._processing_policy = processing_policy
        self._load_shedding_policy = load_shedding_policy
        self._intra_key_priortization = lambda keys: random.choice(keys)
        if not self._lazy:
            for _ in range(num_worker_threads):
                self._thread_pool.submit(self._worker)

        # Parent tables (source of updates)
        self._parents = []
        # Child tables (descendants who recieve updates)
        self._children = []

        self._actor_handle = None
        self._shard_idx = 0

        self.proc = psutil.Process()
        self.proc.cpu_percent()

    def set_load_shedding(self, policy_cls, *args, **kwargs):
        self._load_shedding_policy_obj = policy_cls(*args, **kwargs)
        self._load_shedding_policy = self._load_shedding_policy_obj.process

    def set_intra_key_prioritization(self, policy_cls, *args, **kwargs):
        self._intra_key_priortization_obj = policy_cls(*args, **kwargs)
        self._intra_key_priortization = self._intra_key_priortization_obj.choose

    def set_shard_idx(self, shard_idx: int):
        self._shard_idx = shard_idx

    def _process_stat(self):
        return {
            "cpu_percent": self.proc.cpu_percent(),
            "memory_mb": self.proc.memory_info().rss / (1024 * 1024),
        }

    def debug_state(self):
        return {
            "table": self._table.debug_state(),
            "process": self._process_stat(),
            "cache_size": self._cache_size,
            "lazy": self._lazy,
            "thread_pool_size": self._thread_pool._max_workers,
            "queue_size": {k: v.qsize() for k, v in self._events.items()},
        }

    def _worker(self):
        """Continuously processes events."""
        while self._running:
            non_empty_queues = [k for k, v in self._events.items() if v.qsize() > 0]
            if len(non_empty_queues) == 0:
                self._empty_queue_event.wait()
                continue
            chosen_key = self._intra_key_priortization(non_empty_queues)
            event = self._events[chosen_key].get()
            self._empty_queue_event.clear()
            if self._table.schema is not None:
                key = getattr(event.record, self._table.schema.primary_key)
                try:
                    current_record = self._table.point_query(key)
                    if self._load_shedding_policy(event.record, current_record):
                        event.process()
                except KeyError:
                    event.process()
            else:
                event.process()

    @abstractmethod
    def on_record(self, record: Record) -> Optional[Record]:
        pass

    def _on_record_helper(self, record: Record):
        result = self.on_record(record)
        if result is not None:
            if isinstance(result, list):  # multiple output values
                for res in result:
                    self.send(res)
            else:
                self.send(result)

    async def _on_record(self, record: Record):
        """
        Create an event and add it to the queue when a record is recieved. 

        :record: Update record to process 
        """
        event = Event(
            lambda: self._on_record_helper(record), record, self._processing_policy
        )
        key = record.entries[self._table.schema.primary_key]
        self._events[key].put(event)
        self._empty_queue_event.set()

    def send(self, record: Record):
        key = getattr(record, self._table.schema.primary_key)
        # update state table
        self._table.update(record)

        # TODO(peter): move eviction code to an update_record function,
        # as the table may change lazily.
        if self._cache_size > 0:
            self._lru.pop(key, None)
            self._lru[key] = key
            self._table.update(record)

            if len(self._lru) > self._cache_size:
                evict_key = self._lru.popitem(last=False)[0]
                # Evict from parents
                for parent in self._parents:
                    parent.choose_actor(evict_key).evict.remote(evict_key)
                for child in self._children:
                    child.choose_actor(evict_key).evict.remote(evict_key)

        record._source = self._actor_handle
        # Network optimization: only send to non-lazy children.
        for child in filter(lambda c: not c.is_lazy(), self._children):
            child.choose_actor(key)._on_record.remote(record)

    def evict(self, key: str):
        self._table.delete(key)
        self._lru.pop(key, None)

    def set_parents(self, parents: List[ActorPool]):
        self._parents = parents

    def set_children(self, children: List[ActorPool]):
        self._children = children

    def get_children(self) -> List[ActorPool]:
        return self._children

    def get_parents(self) -> List[ActorPool]:
        return self._parents

    def is_lazy(self) -> bool:
        return self._lazy

    def set_current_actor_handle(self, actor_handle: ActorHandle):
        self._actor_handle = actor_handle

    def get_schema(self) -> Schema:
        return self._table.schema

    # Table data query functions

    async def get(self, key: str):
        if self._lazy:
            # Bug: stateful operators that produce output dependent on an
            # ordered lineage of parent records.
            parent_records = await asyncio.gather(
                *[parent.get_async(key) for parent in self.get_parents()]
            )

            # Force the thread pool to quickly service the requests.
            # TODO: submit via the events queue to prioritize requests.
            futures = []
            for parent_record in parent_records:
                task = self._thread_pool.submit(self._on_record_helper, parent_record)
                futures.append(asyncio.wrap_future(task))
            await asyncio.gather(*futures)

        record = self._table.point_query(key)
        return record

    def get_all(self):
        # TODO: Generate missing values
        return self._table.bulk_query()
