import os
import stat
import tempfile
import time
from abc import abstractmethod
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Deque, Dict, Iterable, List, Optional, Type, Union

from typing_extensions import Literal

from ralf.v2.manager import LocalManager, RalfManager, RayManager, SimpyManager
from ralf.v2.operator import OperatorConfig
from ralf.v2.record import Record
from ralf.v2.scheduler import FIFO, BaseScheduler, SourceScheduler
from ralf.v2.utils import MERGE_DB_SCRIPTS, get_logger

SUPPORTED_DEPLOY_MODES = {
    "local": LocalManager,
    "ray": RayManager,
    "simpy": SimpyManager,
}

logger = get_logger()


@dataclass
class RalfConfig:
    """Configuration for Ralf application"""

    deploy_mode: Union[Literal["local"], Literal["ray"], Literal["simpy"]]

    metrics_dir: Optional[str] = None

    def __post_init__(self):
        assert self.deploy_mode.lower() in SUPPORTED_DEPLOY_MODES
        if self.metrics_dir is None:
            path = Path(tempfile.gettempdir()) / "ralf_metrics" / str(int(time.time()))
            path.mkdir(parents=True, exist_ok=True)
            self.metrics_dir = str(path)

        # Embed a script to merge metrics databases
        script_path = os.path.join(self.metrics_dir, "merge_db.sh")
        with open(script_path, "w") as f:
            f.write(MERGE_DB_SCRIPTS)
        os.chmod(script_path, stat.S_IEXEC | os.stat(script_path).st_mode)

        logger.msg(f"Using deployment mode {self.deploy_mode}")
        logger.msg(f"Using metrics directory {self.metrics_dir}")

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

    def __repr__(self):
        return self.__class__.__name__


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
        logger.msg(
            "Created FeatureFrame",
            transform=transform_object,
            scheduler=scheduler,
            config=operator_config
            if operator_config != OperatorConfig()
            else "default",
        )

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
        return f"FeatureFrame({repr(self.transform_object)})"


class RalfApplication:
    """An end to end feature processing pipeline in Ralf."""

    def __init__(self, config: RalfConfig):
        self.config = config
        self.manager: RalfManager = self.config.get_manager_cls()(self.config)
        # used by walking graph for deploy
        self.source_frame: Optional[FeatureFrame] = None

    def source(
        self,
        transform_object: BaseTransform,
        operator_config: OperatorConfig = OperatorConfig(),
    ) -> FeatureFrame:
        """Create a source feature frame with source transformation."""

        # TODO: support multiple sources

        if self.source_frame is not None:
            raise NotImplementedError("Multiple source is not supported.")
        self.source_frame = FeatureFrame(
            transform_object, SourceScheduler(), operator_config=operator_config
        )
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
        return self.manager.wait()
