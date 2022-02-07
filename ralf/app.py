from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
import enum
import logging
from optparse import Option
import queue
from threading import Thread
import threading
import time
from graphlib import TopologicalSorter
from typing import Any, Deque, Dict, Iterable, List, Optional, Type, Union


# class Record:
#     pass

Record = int


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
            pass
            # return RayManager


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
        if isinstance(record, StopIteration):
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
        return Record()


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


class RalfOperator:
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

        self.worker_thread = Thread(target=self.run_forever, daemon=True)
        self.worker_thread.start()

    def run_forever(self):
        while True:
            # TODO(simon): consider caching event so we don't need to block on pop
            next_event = self.scheduler.pop_event()
            if isinstance(next_event, threading.Event):
                next_event.wait()
                next_event = self.scheduler.pop_event()
                # print(next_event)
                # assert isinstance(next_event, Record)
            if isinstance(next_event, StopIteration):
                break
            try:
                self.handle_event(next_event)
            except StopIteration:
                for children in self.childrens:
                    children.enqueue_event(StopIteration())
                break
            except Exception:
                logging.exception("error in handling event")
        print("thread exit")

    def handle_event(self, record: Record):
        # exception handling
        out = self.transform_object.on_event(record)
        if isinstance(out, Record):
            self.broadcast_children(out)
        else:  # handle iterables, and None
            pass

    def enqueue_event(
        self, record: Union[Record, StopIteration]
    ):  # TODO(simon): wrap this union type into a data class
        self.scheduler.push_event(record)

    def broadcast_children(self, record):
        for children in self.childrens:
            children.enqueue_event(record)


class RayOperator(LocalOperator):
    def broadcast_children(self, record):
        for children in self.childrens:
            children.enqueue_event.remote(record)


class RalfManager(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def deploy(self):
        pass

    @abstractmethod
    def wait(self):
        pass


class LocalManager(RalfManager):
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
    app = RalfApplication(RalfConfig(deploy_mode="local"))

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
            self.state += record
            print(self.state)
            time.sleep(0.2)
            return None

    sink = app.source(CounterSource(10)).transform(Sum(), LIFO())
    app.deploy()
    app.wait()
