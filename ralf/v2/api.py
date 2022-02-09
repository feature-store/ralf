from abc import abstractmethod
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Iterable, List, Optional, Type, Union

from typing_extensions import Literal

from ralf.v2.manager import LocalManager, RalfManager, RayManager
from ralf.v2.operator import OperatorConfig
from ralf.v2.record import Record
from ralf.v2.scheduler import FIFO, BaseScheduler, SourceScheduler

SUPPORTED_DEPLOY_MODES = {
    "local": LocalManager,
    "ray": RayManager,
    "simpy": RalfManager,
}


@dataclass
class RalfConfig:
    """Configuration for Ralf application"""

    deploy_mode: Union[Literal["local"], Literal["ray"], Literal["simpy"]]

    # TODO(simon): implement metrics tracking
    # metrics_dir: Optional[str] = None

    def __post_init__(self):
        assert self.deploy_mode.lower() in SUPPORTED_DEPLOY_MODES

    def get_manager_cls(self) -> Type[RalfManager]:
        return SUPPORTED_DEPLOY_MODES[self.deploy_mode]


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
    """Encapsulate a feature transformation and its related policies configuration."""

    def __init__(
        self,
        transform_object: BaseTransform,
        scheduler: BaseScheduler = FIFO(),
        operator_config: OperatorConfig = OperatorConfig(),
    ):
        self.transform_object = transform_object
        self.scheduler = scheduler
        self.config = operator_config
        self.children: List["FeatureFrame"] = []

    def transform(
        self,
        transform_object: BaseTransform,
        scheduler: BaseScheduler = FIFO(),
        operator_config: OperatorConfig = OperatorConfig(),
    ) -> "FeatureFrame":
        """Apply a transformation to this feature frame, with scheduler."""
        frame = FeatureFrame(transform_object, scheduler, operator_config)
        self.children.append(frame)
        return frame

    def __repr__(self) -> str:
        return f"FeatureFrame({self.transform_object})"


class RalfApplication:
    """An end to end feature processing pipeline in Ralf."""

    def __init__(self, config: RalfConfig):
        self.config = config
        self.manager: RalfManager = self.config.get_manager_cls()()
        # used by walking graph for deploy
        self.source_frame: Optional[FeatureFrame] = None

    def source(self, transform_object: BaseTransform) -> FeatureFrame:
        """Create a source feature frame with source transformation."""
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
        """Deloy the ralf dataflow to the specified manager in config."""
        graph = self._walk_full_graph()
        self.manager.deploy(graph)

    def wait(self):
        """Wait for the data processing to finish."""
        self.manager.wait()
