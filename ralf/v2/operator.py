import asyncio
import hashlib
import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from threading import Thread
from typing import TYPE_CHECKING, Any, Dict, List, Tuple, Type

import ray
import simpy
from ray.actor import ActorHandle

from ralf.v2.record import Record
from ralf.v2.scheduler import DummyEntry, WakerProtocol

if TYPE_CHECKING:
    from ralf.v2.api import BaseTransform, FeatureFrame


class RalfOperator(ABC):
    """Base class for actually deploying transform objects."""

    @abstractmethod
    def __init__(
        self,
        frame: "FeatureFrame",
        children: List["RalfOperator"],
    ):
        pass

    @abstractmethod
    def enqueue_event(self, record: Record):
        pass

    def dump_transform_state(self) -> List["BaseTransform"]:
        pass

    def _send_to_children(self, records: List[Record]):
        for record in records:
            for children in self.children:
                children.enqueue_event(record)


class LocalOperator(RalfOperator):
    """Run a transform locally."""

    def __init__(
        self,
        frame: "FeatureFrame",
        children: List["RalfOperator"],
    ):
        self.frame = frame
        self.transform_object = self.frame.transform_object
        self.children = children
        self.scheduler = frame.scheduler

        self.worker_thread = Thread(target=self._run_forever, daemon=True)
        self.worker_thread.start()

    def _run_forever(self):
        while True:
            next_event = self.scheduler.pop_event()

            # Wait for record here
            if next_event.is_wait_event():
                next_event.wait()
                next_event = self.scheduler.pop_event()
                assert not next_event.is_wait_event()

            try:
                # Process the record
                if next_event.is_data():
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
                    raise StopIteration()

                else:
                    raise Exception(
                        "Unknown record type, it should be either data or stop iteration."
                    )
            except StopIteration:
                self._send_to_children([Record.make_stop_iteration()])
                break
            # Application error, log and continue
            except Exception:
                logging.exception("error in handling event")

    def enqueue_event(self, record: Record):
        self.local_handle_event(record)

    def local_handle_event(self, record: Record):
        self.scheduler.push_event(record)

    def dump_transform_state(self) -> List["BaseTransform"]:
        return [self.transform_object]


@ray.remote
class RayLocalOperator(LocalOperator):
    async def wait_for_exit(self):
        while True:
            await asyncio.sleep(1)
            if not self.worker_thread.is_alive():
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

    def __init__(self, frame: "FeatureFrame", children: List["RalfOperator"]):
        self.pool = OperatorActorPool.make_replicas(
            num_replicas=frame.config.ray_config.num_replicas,
            actor_class=RayLocalOperator,
            init_kwargs={"frame": frame, "children": children},
        )
        self.children = children

    def enqueue_event(self, record: Record):
        if record.is_data():
            # TODO(simon): shard by key instead?
            self.pool.choose_actor(repr(record)).local_handle_event.remote(record)
        elif record.is_stop_iteration():  # broadcast termination event
            [actor.local_handle_event.remote(record) for actor in self.pool.handles]
        else:
            raise Exception(f"Can't enqueue_event for event type {record.type_}")

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


class SimpySink(RalfOperator):
    def __init__(self):
        self.recordings = []

    def enqueue_event(self, record: Record):
        self.recordings.append(record)

    def dump_transform_state(self):
        return self.recordings


class SimpyOperator(RalfOperator):
    """Skip running transform, but record the event ordering"""

    def __init__(self, frame: "FeatureFrame", children: List["RalfOperator"]):
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

    def enqueue_event(self, record: Record):
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
