from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
import logging
from optparse import Option
import queue
from threading import Thread
import threading
from graphlib import TopologicalSorter
from typing import Any, Deque, Dict, Iterable, List, Optional, Union


# class Record:
#     pass

Record = int


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
    def __init__(self, transform_object: BaseTransform):
        self.transform_object = transform_object
        self.children: List["FeatureFrame"] = []

    def transform(self, transform_object: BaseTransform) -> "FeatureFrame":
        frame = FeatureFrame(transform_object)
        self.children.append(frame)
        return frame

    def __repr__(self) -> str:
        return f"FeatureFrame({self.transform_object})"


class BaseScheduler(ABC):
    """Base class for scheduling event on to transform operator"""

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
        if self.waker is not None:
            self.waker.set()
            self.waker = None
        return self.queue.append(record)

    def pop_event(self) -> Union[Record, threading.Event]:
        if len(self.queue) == 0:
            assert self.waker is None
            self.waker = threading.Event()
            return self.waker
        return self.queue.pop(0)


class InfiniteNoopEvent(BaseScheduler):
    def push_event(self, record: Record):
        pass

    def pop_event(self) -> Union[Record, threading.Event]:
        return Record()


class RalfOperator:
    pass


class LocalOperator(RalfOperator):
    def __init__(
        self,
        frame: FeatureFrame,
        childrens: List["RalfOperator"],
        scheduler: BaseScheduler,
    ):
        self.frame = frame
        self.transform_object = self.frame.transform_object
        self.childrens = childrens
        self.scheduler = scheduler

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
            for children in self.childrens:
                children.enqueue_event(out)
        else:  # handle iterables, and None
            pass

    def enqueue_event(
        self, record: Union[Record, StopIteration]
    ):  # TODO(simon): wrap this union type into a data class
        self.scheduler.push_event(record)


@dataclass
class RalfConfig:
    metrics_dir: Optional[str] = None


class RalfApplication:
    def __init__(self, config: RalfConfig):
        self.config = config
        self.source_frame: Optional[FeatureFrame] = None
        self.operators: Dict[FeatureFrame, RalfOperator] = dict()

    def source(self, transform_object: BaseTransform) -> FeatureFrame:
        if self.source_frame is not None:
            raise NotImplementedError("Multiple source is not supported.")
        self.source_frame = FeatureFrame(transform_object)
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
        sorted_order = list(TopologicalSorter(graph).static_order())
        for frame in sorted_order:
            is_source = frame is self.source_frame
            operator = LocalOperator(
                frame,
                childrens=[self.operators[f] for f in graph[frame]],
                scheduler=InfiniteNoopEvent() if is_source else FIFO(),
            )
            self.operators[frame] = operator

    def run(self):
        self.deploy()

        # Make the join condition configurable and agnostic to operator implementation
        for operator in self.operators.values():
            operator.worker_thread.join()


if __name__ == "__main__":
    app = RalfApplication(RalfConfig())

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
            return None

    sink = app.source(CounterSource(10)).transform(Sum())
    app.run()
