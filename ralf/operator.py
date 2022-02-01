import asyncio
import hashlib
import random
import threading
from abc import ABC, abstractmethod
from collections import OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor
from queue import PriorityQueue
from typing import Callable, List, Optional
from functools import wraps

import psutil
import ray
from ray.actor import ActorHandle
from event_metrics import MetricConnection

from ralf.policies import load_shedding_policy, processing_policy
from ralf.state import Record
from ralf.tables.table_state import TableState
from ralf.tables import Connector


class TransformWrapper:
    def __init__(
        self,
        user_class,
        connector: Connector,
    ):
        # Mained output table state
        self._table = TableState(schema, connector)
        self._cache_size = cache_size
        self._lru = OrderedDict()
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

        self.proc = None

        threading.Thread(target=self.collect_metrics, daemon=True)

    def collect_metrics(self):
        if self.proc is None:
            self.proc = psutil.Process()
            self.proc.cpu_percent()

        return {
            "table": self._table.debug_state(),
            "process": {
                "cpu_percent": self.proc.cpu_percent(),
                "memory_mb": self.proc.memory_info().rss / (1024 * 1024),
            },
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
