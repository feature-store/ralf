import abc
import itertools
from typing import Dict, List, Tuple, Type
from dataclasses import dataclass

import simpy

from ralf.simulation.priority_queue import PerKeyPriorityQueue

KeyType = int


class CrossKeyLoadBalancer(abc.ABC):
    @abc.abstractmethod
    def choose(self, per_key_queues: Dict[KeyType, PerKeyPriorityQueue]) -> KeyType:
        pass


class RoundRobinLoadBalancer(CrossKeyLoadBalancer):
    """Simple policy that cycle through all the keys fairly"""

    def __init__(self):
        self.cur_key_set = set()
        self.cur_key_iter = None

    def choose(self, per_key_queues: Dict[KeyType, PerKeyPriorityQueue]) -> KeyType:
        key_set = set(per_key_queues.keys())
        if key_set != self.cur_key_set:
            self.cur_key_set = key_set
            self.cur_key_iter = itertools.cycle(key_set)

        key = next(self.cur_key_iter)
        # TODO(simon): maybe do a "peak" here to trigger eviction policies
        return key


@dataclass
class PlanEntry:
    # Map operator attribute
    processing_time: float

    # Event Attribute
    window_start_seq_id: int
    window_end_seq_id: int
    key: int


class RalfMapper:
    def __init__(
        self,
        env: simpy.Environment,
        source_queues: Dict[KeyType, PerKeyPriorityQueue],
        key_selection_policy_cls: Type[CrossKeyLoadBalancer],
        model_run_time_s: float,
    ) -> None:
        self.env = env
        self.source_queues = source_queues
        self.key_selection_policy = key_selection_policy_cls()
        self.model_runtime_s = model_run_time_s
        self.env.process(self.run())

        self.plan: List[PlanEntry] = []

    def run(self):
        while True:
            # windows = yield self.source_queue.get()
            chosen_key = self.key_selection_policy.choose(self.source_queues)
            windows = yield self.source_queues[chosen_key].get()
            print(
                f"at time {self.env.now:.2f}, RalfMapper should work on {windows} (last timestamp)"
            )
            self.plan.append(
                PlanEntry(
                    round(self.env.now, 6),
                    windows.window[0].seq_id,
                    windows.window[-1].seq_id,
                    windows.window[0].key,  # key will be same anyway
                ).__dict__
            )
            yield self.env.timeout(self.model_runtime_s)
