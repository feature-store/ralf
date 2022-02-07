import enum
import hashlib
import logging
import queue
import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from copy import copy
from dataclasses import dataclass, field
from graphlib import TopologicalSorter
from optparse import Option
from threading import Thread
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple, Type, Union

import ray
from ray.actor import ActorHandle

# class Record:
#     pass


@dataclass
class Record:
    # Tagged union
    type_: str

    # data type
    entries: int = 0

    # error type
    error: Exception = StopIteration()


@dataclass
class RalfConfig:
    metrics_dir: Optional[str] = None
    deploy_mode: str = "local"

    def __post_init__(self):
        self.deploy_mode = self.deploy_mode.lower()
        assert self.deploy_mode in {
            "local",
            "ray",
            # "simpy",
        }

    def get_manager_cls(self) -> Type["RalfManager"]:
        if self.deploy_mode == "local":
            return LocalManager
        elif self.deploy_mode == "ray":
            return RayManager


class BaseScheduler(ABC):
    """Base class for scheduling event on to transform operator"""

    def wake_waiter_if_needed(self):
        if self.waker is not None:
            self.waker.set()
            self.waker = None

    def new_waker(self):
        assert self.waker is None
        self.waker = threading.Event()
        return self.waker

    @abstractmethod
    def push_event(self, record: Record):
        pass

    @abstractmethod
    def pop_event(self) -> Union[Record, threading.Event]:
        pass


class FIFO(BaseScheduler):
    def __init__(self) -> None:
        self.queue: List[Record] = []
        self.waker: Optional[threading.Event] = None

    def push_event(self, record: Record):
        self.wake_waiter_if_needed()
        self.queue.append(record)

    def pop_event(self) -> Union[Record, threading.Event]:
        if len(self.queue) == 0:
            return self.new_waker()
        return self.queue.pop(0)


class LIFO(BaseScheduler):
    def __init__(self) -> None:
        self.queue: List[Record] = []
        self.waker: Optional[threading.Event] = None

    def push_event(self, record: Record):
        self.wake_waiter_if_needed()
        if record.type_ == "error":
            self.queue.insert(0, record)
        else:
            self.queue.append(record)

    def pop_event(self) -> Union[Record, threading.Event]:
        if len(self.queue) == 0:
            return self.new_waker()
        return self.queue.pop(-1)


class SourceScheduler(BaseScheduler):
    def push_event(self, record: Record):
        pass

    def pop_event(self) -> Union[Record, threading.Event]:
        return Record("entry")


class BaseTransform:
    @abstractmethod
    def on_event(self, record: Record) -> Union[None, Record, Iterable[Record]]:
        """Emit none, one, or multiple events given incoming record.

        When StopIteration exception is raised, the transform process will terminate.
        A StopIteration special record will also propogate to downstream feature frames.

        Source operator should expect this method to be called continously unless it raises
        StopIteration.
        """
        raise NotImplementedError("To be implemented by subclass.")


class FeatureFrame:
    def __init__(
        self, transform_object: BaseTransform, scheduler: BaseScheduler = FIFO()
    ):
        self.transform_object = transform_object
        self.scheduler = scheduler
        self.children: List["FeatureFrame"] = []

    def transform(
        self, transform_object: BaseTransform, scheduler: BaseScheduler = FIFO()
    ) -> "FeatureFrame":
        frame = FeatureFrame(transform_object, scheduler)
        self.children.append(frame)
        return frame

    def __repr__(self) -> str:
        return f"FeatureFrame({self.transform_object})"


class RalfOperator(ABC):
    @abstractmethod
    def __init__(
        self,
        frame: FeatureFrame,
        childrens: List["RalfOperator"],
    ):
        pass

    @abstractmethod
    def enqueue_event(self, record: Record):
        pass


class LocalOperator(RalfOperator):
    def __init__(
        self,
        frame: FeatureFrame,
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
            # TODO(simon): consider caching event so we don't need to block on pop
            next_event = self.scheduler.pop_event()
            if isinstance(next_event, threading.Event):
                next_event.wait()
                next_event = self.scheduler.pop_event()
                # print(next_event)
                assert isinstance(next_event, Record)
            if next_event.type_ == "error":
                break
            try:
                self._handle_event(next_event)
            except StopIteration:
                for children in self.childrens:
                    children.enqueue_event(Record("error"))
                break
            except Exception:
                logging.exception("error in handling event")
        print("thread exit")

    def _handle_event(self, record: Record):
        # exception handling
        out = self.transform_object.on_event(record)
        # TODO(simon): handle other out type like None or Iterables.
        for children in self.childrens:
            children.enqueue_event(Record("entry", entries=out))

    def enqueue_event(self, record: Record):
        self.handle_event(record)

    def handle_event(self, record: Record):
        self.scheduler.push_event(record)


@dataclass
class RalfContext:
    current_shard_idx: int = -1


class OperatorActorPool:
    """Contains a set number of Ray Actors."""

    def __init__(self, handles: List[ActorHandle]):
        self.handles = handles

    @classmethod
    def make_replicas(
        cls: Type["OperatorActorPool"],
        num_replicas: int,
        actor_class: Type,
        ralf_context: RalfContext,
        actor_options: Dict[str, Any] = dict(),
        init_args: Tuple[Any] = tuple(),
        init_kwargs: Dict[str, Any] = dict(),
    ):
        assert num_replicas > 0

        handles = []
        for i in range(num_replicas):
            ralf_context = copy(ralf_context)
            ralf_context.current_shard_idx = i

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

    # def broadcast(self, attr, *args):
    #     # TODO: fix having to wait
    #     outputs = []
    #     for handle in self.handles:
    #         outputs.append(getattr(handle, attr).remote(*args))
    #     return outputs


class RayOperator(RalfOperator):
    def __init__(self, frame: FeatureFrame, childrens: List["RalfOperator"]):
        self.pool = OperatorActorPool.make_replicas(
            num_replicas=1,
            actor_class=ray.remote(LocalOperator),
            ralf_context=RalfContext(),
            init_kwargs={"frame": frame, "childrens": childrens},
        )
        self.children = childrens

    def enqueue_event(self, record: Record):
        self.pool.choose_actor(repr(record)).handle_event.remote(record)


class RalfManager(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def deploy(self, graph: Dict[FeatureFrame, List[FeatureFrame]]):
        pass

    @abstractmethod
    def wait(self):
        pass


class RayManager(ABC):
    def __init__(self):
        if not ray.is_initialized():
            ray.init()
        self.operators: Dict[FeatureFrame, RalfOperator] = dict()

    def deploy(self, graph: Dict[FeatureFrame, List[FeatureFrame]]):
        sorted_order: List[FeatureFrame] = list(TopologicalSorter(graph).static_order())
        for frame in sorted_order:
            operator = RayOperator(
                frame,
                childrens=[self.operators[f] for f in graph[frame]],
            )
            self.operators[frame] = operator

    def wait(self):
        # TODO(simon): implement signal waiting for actors.
        # TODO(simon): might require local operators to aware of the exit callback?
        while True:
            time.sleep(5)


class LocalManager(RalfManager):
    # TODO(simon): looks like we can just merge all Manager subclass via parameterizing this?
    operator_cls = LocalOperator

    def __init__(self) -> None:
        self.operators: Dict[FeatureFrame, RalfOperator] = dict()

    def deploy(self, graph: Dict[FeatureFrame, List[FeatureFrame]]):
        sorted_order: List[FeatureFrame] = list(TopologicalSorter(graph).static_order())
        for frame in sorted_order:
            operator = LocalOperator(
                frame,
                childrens=[self.operators[f] for f in graph[frame]],
            )
            self.operators[frame] = operator

    def wait(self):
        # Make the join condition configurable and agnostic to operator implementation
        for operator in self.operators.values():
            operator.worker_thread.join()


class RalfApplication:
    def __init__(self, config: RalfConfig):
        self.config = config
        self.manager: RalfManager = self.config.get_manager_cls()()
        # used by walking graph for deploy
        self.source_frame: Optional[FeatureFrame] = None

    def source(self, transform_object: BaseTransform) -> FeatureFrame:
        if self.source_frame is not None:
            raise NotImplementedError("Multiple source is not supported.")
        self.source_frame = FeatureFrame(transform_object, SourceScheduler())
        return self.source_frame

    def _walk_full_graph(self) -> Dict[FeatureFrame, List[FeatureFrame]]:
        assert self.source_frame is not None

        graph = {}
        queue: Deque[FeatureFrame] = deque([self.source_frame])
        while len(queue) != 0:
            frame = queue.popleft()
            queue.extend(frame.children)
            graph[frame] = frame.children
        return graph

    def deploy(self):
        graph = self._walk_full_graph()
        self.manager.deploy(graph)

    def wait(self):
        self.manager.wait()


if __name__ == "__main__":
    app = RalfApplication(RalfConfig(deploy_mode="ray"))

    class CounterSource(BaseTransform):
        def __init__(self, up_to: int) -> None:
            self.count = 0
            self.up_to = up_to

        def on_event(self, record: Record) -> Union[None, Record, Iterable[Record]]:
            self.count += 1
            if self.count >= self.up_to:
                raise StopIteration()
            return self.count

    class Sum(BaseTransform):
        def __init__(self) -> None:
            self.state = 0

        def on_event(self, record: Record) -> Union[None, Record, Iterable[Record]]:
            self.state += record.entries
            print(self.state)
            time.sleep(0.2)
            return None

    sink = app.source(CounterSource(10)).transform(Sum(), LIFO())
    app.deploy()
    app.wait()
