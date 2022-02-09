import asyncio
import hashlib
import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from threading import Thread
from typing import TYPE_CHECKING, Any, Dict, List, Tuple, Type

import ray
from ray.actor import ActorHandle

from ralf.v2.record import Record

if TYPE_CHECKING:
    from ralf.v2.api import BaseTransform, FeatureFrame


class RalfOperator(ABC):
    """Base class for actually deploying transform objects."""

    @abstractmethod
    def __init__(
        self,
        frame: "FeatureFrame",
        childrens: List["RalfOperator"],
    ):
        pass

    @abstractmethod
    def enqueue_event(self, record: Record):
        pass

    def dump_transform_state(self) -> List["BaseTransform"]:
        pass


class LocalOperator(RalfOperator):
    """Run a transform locally."""

    def __init__(
        self,
        frame: "FeatureFrame",
        childrens: List["RalfOperator"],
    ):
        self.frame = frame
        self.transform_object = self.frame.transform_object
        self.childrens = childrens
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

    def _send_to_children(self, records: List[Record]):
        for record in records:
            for children in self.childrens:
                children.enqueue_event(record)

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

    def __init__(self, frame: "FeatureFrame", childrens: List["RalfOperator"]):
        self.pool = OperatorActorPool.make_replicas(
            num_replicas=frame.config.ray_config.num_replicas,
            actor_class=RayLocalOperator,
            init_kwargs={"frame": frame, "childrens": childrens},
        )
        self.children = childrens

    def enqueue_event(self, record: Record):
        self.pool.choose_actor(repr(record)).local_handle_event.remote(record)

    def dump_transform_state(self) -> List["BaseTransform"]:
        return sum(
            ray.get(
                [handle.dump_transform_state.remote() for handle in self.pool.handles]
            ),
            [],
        )


@dataclass
class RayOperatorConfig:
    num_replicas: int = 1


@dataclass
class OperatorConfig:
    ray_config: RayOperatorConfig = RayOperatorConfig()
