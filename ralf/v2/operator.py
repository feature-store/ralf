import asyncio
import hashlib
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Thread
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type

import event_metrics
import ray
import simpy
import structlog
from ray.actor import ActorHandle

from ralf.v2.record import Record
from ralf.v2.scheduler import DummyEntry, WakerProtocol
from ralf.v2.utils import get_logger, set_metrics_conn

if TYPE_CHECKING:
    from ralf.v2.api import BaseTransform, FeatureFrame, RalfConfig

logger = get_logger()


class RalfOperator(ABC):
    """Base class for actually deploying transform objects."""

    @abstractmethod
    def __init__(
        self,
        frame: "FeatureFrame",
        children: List["RalfOperator"],
        ralf_config: "RalfConfig",
    ):
        pass

    @abstractmethod
    def enqueue_events(self, records: List[Record]):
        pass

    @abstractmethod
    def get(self, key) -> Record:
        pass

    def dump_transform_state(self) -> List["BaseTransform"]:
        pass

    def _send_to_children(self, records: List[Record]):
        for children in self.children:
            children.enqueue_events(records)


class LocalOperator(RalfOperator):
    """Run a transform locally."""

    def __init__(
        self,
        frame: "FeatureFrame",
        children: List["RalfOperator"],
        ralf_config: "RalfConfig",
        context: Optional[Dict] = {"shard_idx": 0},
    ):
        self.frame = frame
        self.transform_object = self.frame.transform_object
        self.children = children
        self.scheduler = frame.scheduler
        self.scheduler._operator = self  # set operator for scheduler
        self.context = context
        self.config = ralf_config

        structlog.threadlocal.bind_threadlocal(
            frame=self.frame,
            context=self.context,
            thread="main",
        )

        logger.msg("Preparing transform object")
        self.frame.prepare()
        self.worker_thread = Thread(target=self._run_forever, daemon=True)
        self.worker_thread.start()

    def _run_forever(self):
        structlog.threadlocal.bind_threadlocal(
            frame=self.frame,
            context=self.context,
            thread="worker",
        )

        # self.frame.prepare()        

        db_path = f"{self.config.metrics_dir}/{str(self.frame.transform_object)}_{self.context['shard_idx']}.db"
        metrics_connection = event_metrics.MetricConnection(
            db_path,
            default_labels={
                **self.context,
                **dict(transform=str(self.frame.transform_object)),
            },
        )
        logger.msg(f"Store metrics at {db_path}")
        set_metrics_conn(metrics_connection)

        error_count = 0
        max_error_count = 10

        @contextmanager
        def time_and_count(metric_name, labels={}):
            start_ns = time.time_ns()
            yield
            duration_ns = time.time_ns() - start_ns
            # metrics_connection.observe(
            #     f"{metric_name}_duration_ns", duration_ns, labels=labels
            # )
            # metrics_connection.increment(f"{metric_name}_counter", labels=labels)

        while True:
            with time_and_count("pop_event"):
                next_event = self.scheduler.pop_event()

                # Wait for record here
                if not isinstance(next_event, list) and next_event.is_wait_event():
                    with time_and_count("wait_pop_event"):
                        next_event.wait()
                        next_event = self.scheduler.pop_event()
                        assert (
                            isinstance(next_event, list)
                            or not next_event.is_wait_event()
                        )

            metrics_connection.observe("queue_size", self.scheduler.qsize())
            try:
                # Process the record
                if isinstance(next_event, list) or next_event.is_data():
                    with time_and_count("transform_on_event"):
                        if isinstance(next_event, list):
                            out = self.transform_object.on_events(next_event)
                        else:
                            out = self.transform_object.on_event(next_event)
                    if isinstance(out, Record):
                        self._send_to_children([out])
                    elif isinstance(out, Sequence):
                        self._send_to_children(list(out))
                    else:
                        assert (
                            out is None
                        ), f"Transform output must be Record, Sequence[Record], or None. It is {out}"

                # If this operator got StopIteration, it should exit
                elif next_event.is_stop_iteration():
                    self.transform_object.on_stop(next_event)
                    raise StopIteration()

                else:
                    raise Exception(
                        "Unknown record type, it should be either data or stop iteration."
                    )
            except StopIteration:
                logger.msg(
                    "operator received StopIteration, propogating and exiting..."
                )
                self._send_to_children([Record.make_stop_iteration()])
                break
            # Application error, log and continue
            except Exception:
                error_count += 1
                logger.msg("error in handling event", error_count=error_count)
                logger.exception("exception", exc_info=True)
                if error_count == max_error_count:
                    logger.msg("operator has errored too much, exiting")
                    self._send_to_children([Record.make_stop_iteration()])
                    break

    def enqueue_events(self, records: List[Record]):
        self.local_handle_events(records)

    def local_handle_events(self, records: List[Record]):
        for record in records:
            self.scheduler.push_event(record)

    def get(self, key):
        return self.transform_object.get(key)

    def dump_transform_state(self) -> List["BaseTransform"]:
        return [self.transform_object]


@ray.remote
class RayLocalOperator(LocalOperator):
    async def wait_for_exit(self):
        while True:
            await asyncio.sleep(1)
            if not self.worker_thread.is_alive():
                logger.msg("Worker thread exited!", frame=self.frame)
                break


class OperatorActorPool:
    """Contains a set number of Ray Actors."""

    def __init__(self, handles: List[ActorHandle]):
        self.handles = handles

    @classmethod
    def make_replicas(
        cls: Type["OperatorActorPool"],
        num_replicas: int,
        actor_class: Type,
        actor_options: Dict[str, Any] = dict(),
        init_args: Tuple[Any] = tuple(),
        init_kwargs: Dict[str, Any] = dict(),
    ):
        assert num_replicas > 0

        handles = []
        for i in range(num_replicas):
            init_kwargs["context"] = {"shard_idx": i}
            handle = actor_class.options(**actor_options).remote(
                *init_args, **init_kwargs
            )
            handles.append(handle)

        return cls(handles)

    def hash_key(self, key: str) -> int:
        assert isinstance(key, str)
        hash_val = hashlib.sha1(key.encode("utf-8")).hexdigest()
        return int(hash_val, 16)

    def choose_actor(self, key: str) -> ActorHandle:
        return self.handles[self.hash_key(key) % len(self.handles)]


class RayOperator(RalfOperator):
    """Run transforms as part of sharded actor pool."""

    def __init__(
        self,
        frame: "FeatureFrame",
        children: List["RalfOperator"],
        ralf_config: "RalfConfig",
    ):
        self.frame = frame
        self.pool = OperatorActorPool.make_replicas(
            num_replicas=frame.config.ray_config.num_replicas,
            actor_class=RayLocalOperator,
            init_kwargs={
                "frame": frame,
                "children": children,
                "ralf_config": ralf_config,
            },
        )
        self.children = children

    def _check_sharding_key_exist_if_necessary(self, records: List[Record]):
        if len(records) == 0:
            return
        r = records[0]
        if r.is_data() and len(self.pool.handles) > 1:
            assert (
                r.shard_key != ""
            ), f"{self.frame} is sharded but no shard key preset."

    def enqueue_events(self, records: List[Record]):
        self._check_sharding_key_exist_if_necessary(records)

        actor_map = defaultdict(list)
        for record in records:
            if record.is_data():
                actor_map[self.pool.choose_actor(record.shard_key)].append(record)
            elif record.is_stop_iteration():  # broadcast termination event
                [
                    actor.local_handle_events.remote([record])
                    for actor in self.pool.handles
                ]
            else:
                raise Exception(f"Can't enqueue_events for event type {record.type_}")

        # send records to actors
        for handle, replica_records in actor_map.items():
            handle.local_handle_events.remote(replica_records)

    def get(self, key):
        return self.pool.choose_actor(key).get.remote(key)

    def dump_transform_state(self) -> List["BaseTransform"]:
        return sum(
            ray.get(
                [handle.dump_transform_state.remote() for handle in self.pool.handles]
            ),
            [],
        )


@dataclass
class SimTraceRecord:
    request_id: int
    frame: str
    start_time: float
    end_time: float


class _SimpySink(RalfOperator):
    def __init__(self):
        self.recordings = []

    def enqueue_events(self, records: List[Record]):
        self.recordings.extend(records)

    def dump_transform_state(self):
        return self.recordings


class SimpyOperator(RalfOperator):
    """Skip running transform, but record the event ordering"""

    def __init__(
        self,
        frame: "FeatureFrame",
        children: List["RalfOperator"],
        ralf_config: "RalfConfig",
    ):
        self.frame = frame
        self.scheduler = frame.scheduler
        self.config = frame.config.simpy_config
        self.children = children

        self.id = 0
        env = self.config.shared_env
        self.env = env

        class SimulationWaker(WakerProtocol):
            def __init__(self):
                self.simpy_event = env.event()

            def set(self):
                self.simpy_event.succeed()

            def wait(self, _timeout=None):
                raise NotImplementedError("Use `yield waker.simpy_event` instead.")

        self.scheduler.event_class = SimulationWaker
        self.env.process(self.run_simulation_node())

    def run_simulation_node(self):
        while True:
            record = self.scheduler.pop_event()
            if record.is_wait_event():
                yield record.entry.simpy_event
                record = self.scheduler.pop_event()
                assert record.is_data()

            if isinstance(record.entry, DummyEntry):
                # this is from source, let's modify it into trace record
                request_id = self.id
                self.id += 1
            else:
                request_id = record.entry.request_id

            new_record = Record(
                SimTraceRecord(
                    request_id,
                    str(self.frame),
                    self.env.now,
                    self.env.now + self.config.processing_time_s,
                )
            )
            yield self.env.timeout(self.config.processing_time_s)

            self._send_to_children([new_record])

            if self.env.now > self.config.stop_after_s:
                break

    def enqueue_events(self, records: List[Record]):
        for record in records:
            self.scheduler.push_event(record)


@dataclass
class RayOperatorConfig:
    num_replicas: int = 1


@dataclass
class SimpyOperatorConfig:
    shared_env: simpy.Environment = None
    processing_time_s: float = 0.1
    stop_after_s: float = float("inf")

    def __post_init__(self):
        assert self.processing_time_s > 0
        assert self.stop_after_s > 0


@dataclass
class OperatorConfig:
    ray_config: RayOperatorConfig = RayOperatorConfig()
    simpy_config: SimpyOperatorConfig = SimpyOperatorConfig()

    def __post_init__(self):
        assert isinstance(self.ray_config, RayOperatorConfig), type(self.ray_config)
        assert isinstance(self.simpy_config, SimpyOperatorConfig), type(
            self.simpy_config
        )
