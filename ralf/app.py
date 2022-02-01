from abc import abstractmethod
from collections import deque
from dataclasses import dataclass
from optparse import Option
from graphlib import TopologicalSorter
from typing import Deque, Dict, Iterable, List, Optional, Union


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


class RalfOperator:
    pass


class LocalOperator(RalfOperator):
    def __init__(self, frame: FeatureFrame, childrens: List["RalfOperator"]):
        self.frame = frame
        self.transform_object = self.frame.transform_object
        self.childrens = childrens

    def poll(self, record: Record):
        while True:
            try:
                self.handle_event(record)
            except StopIteration:
                print("StopIteration")
                break

    def handle_event(self, record: Record):
        # exception handling
        out = self.transform_object.on_event(record)
        if isinstance(out, Record):
            for children in self.childrens:
                children.handle_event(out)
        else:  # handle iterables
            pass


@dataclass
class RalfConfig:
    metrics_dir: Optional[str] = None


class RalfApplication:
    def __init__(self, config: RalfConfig):
        self.config = config
        self.source_frame: Optional[FeatureFrame] = None
        self.source_operator: Option[RalfOperator] = None

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
        operators = dict()
        for frame in sorted_order:
            print(f"Deploying {frame}")
            operator = LocalOperator(
                frame, childrens=[operators[f] for f in graph[frame]]
            )
            operators[frame] = operator
            if frame is self.source_frame:
                self.source_operator = operator

    def run(self):
        self.deploy()
        self.source_operator.poll(0)


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
