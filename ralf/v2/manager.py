import time
from abc import ABC, abstractmethod
from graphlib import TopologicalSorter
from typing import TYPE_CHECKING, Dict, List, Type

import ray

from ralf.v2.operator import (
    LocalOperator,
    RalfOperator,
    RayOperator,
    SimpyOperator,
    _SimpySink,
)

if TYPE_CHECKING:
    from ralf.v2.api import FeatureFrame, RalfConfig


class RalfManager(ABC):
    """Base class for deployment managers for DAG.

    Subclass should specify the operator_cls as class attribute and implement `wait`.
    """

    operator_cls: Type[RalfOperator]

    def __init__(self, config: "RalfConfig"):
        self.operators: Dict["FeatureFrame", RalfOperator] = dict()
        self.config = config

    def deploy(self, graph: Dict["FeatureFrame", List["FeatureFrame"]]):
        sorted_order: List["FeatureFrame"] = list(
            TopologicalSorter(graph).static_order()
        )
        for frame in sorted_order:
            operator = self.operator_cls(
                frame,
                children=[self.operators[f] for f in graph[frame]],
                ralf_config=self.config,
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

    def __init__(self, config):
        if not ray.is_initialized():
            ray.init("ray://127.0.0.1:10001")
        super().__init__(config)

    def wait(self):
        refs = []
        for operator in self.operators.values():
            for handle in operator.pool.handles:
                refs.append(handle.wait_for_exit.remote())
        while True:
            _, not_done = ray.wait(refs, num_returns=len(refs), timeout=0.5)
            # print("Waiting for", not_done)
            if len(not_done) == 0:
                break
            time.sleep(1)


class SimpyManager(RalfManager):
    """Run schedulers in Simpy environment, skip transforms"""

    operator_cls: Type[RalfOperator] = SimpyOperator

    def deploy(self, graph: Dict["FeatureFrame", List["FeatureFrame"]]):
        super().deploy(graph)

        self.sink = _SimpySink()
        for operator in self.operators.values():
            operator.children.append(self.sink)

    def wait(self):
        return self.sink.dump_transform_state()
