from abc import ABC, abstractmethod
from graphlib import TopologicalSorter
from typing import TYPE_CHECKING, Dict, List, Type

import ray

from ralf.v2.operator import (
    LocalOperator,
    RalfOperator,
    RayOperator,
    SimpyOperator,
    SimpySink,
)

if TYPE_CHECKING:
    from ralf.v2.api import FeatureFrame


class RalfManager(ABC):
    """Base class for deployment managers for DAG.

    Subclass should specify the operator_cls as class attribute and implement `wait`.
    """

    operator_cls: Type[RalfOperator]

    def __init__(self):
        self.operators: Dict["FeatureFrame", RalfOperator] = dict()

    def deploy(self, graph: Dict["FeatureFrame", List["FeatureFrame"]]):
        sorted_order: List["FeatureFrame"] = list(
            TopologicalSorter(graph).static_order()
        )
        for frame in sorted_order:
            operator = self.operator_cls(
                frame,
                children=[self.operators[f] for f in graph[frame]],
            )
            self.operators[frame] = operator

    @abstractmethod
    def wait(self):
        pass


class LocalManager(RalfManager):
    """Run transforms in local process, where each transform has a worker thread."""

    operator_cls = LocalOperator

    def wait(self):
        for operator in self.operators.values():
            operator.worker_thread.join()


class RayManager(RalfManager):
    """Run transforms in Ray actor pools."""

    operator_cls: Type[RalfOperator] = RayOperator

    def __init__(self):
        if not ray.is_initialized():
            ray.init()
        super().__init__()

    def wait(self):
        for operator in self.operators.values():
            for handle in operator.pool.handles:
                ray.wait([handle.wait_for_exit.remote()], num_returns=1)


class SimpyManager(RalfManager):
    """Run schedulers in Simpy environment, skip transforms"""

    operator_cls: Type[RalfOperator] = SimpyOperator

    def __init__(self):
        super().__init__()

    def deploy(self, graph: Dict["FeatureFrame", List["FeatureFrame"]]):
        super().deploy(graph)

        self.sink = SimpySink()
        for operator in self.operators.values():
            operator.children.append(self.sink)

    def wait(self):
        return self.sink.dump_transform_state()
