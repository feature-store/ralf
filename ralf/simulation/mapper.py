import abc
import itertools
import random
from dataclasses import dataclass
from typing import Dict, List, Type
from more_itertools.more import divide

import simpy
from more_itertools import chunked

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
    replica_id: int

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
        num_replicas: int = 1,
    ) -> None:
        self.env = env
        self.total_source_queues = source_queues
        assert len(source_queues) >= num_replicas

        # Shard source queues into each replica's id.
        source_keys = list(source_queues.keys())
        random.shuffle(source_keys)
        self.sharded_keys = dict(
            enumerate(map(list, divide(num_replicas, source_keys)))
        )

        self.key_selection_policy = key_selection_policy_cls 
        self.model_runtime_s = model_run_time_s
        for i in range(num_replicas):
            self.env.process(self.run(replica_id=i))

        self.plan: List[PlanEntry] = []

    def run(self, replica_id: int):
        this_shard_source_queues = {
            key: self.total_source_queues[key] for key in self.sharded_keys[replica_id]
        }
        while True:
            # windows = yield self.source_queue.get()
            chosen_key = self.key_selection_policy.choose(this_shard_source_queues)
            windows = yield self.total_source_queues[chosen_key].get()
            print(
                f"at time {self.env.now:.2f}, RalfMapper {replica_id} should work on {windows} (last timestamp)"
            )
            self.plan.append(
                PlanEntry(
                    round(self.env.now, 6),
                    replica_id,
                    windows.window[0].seq_id,
                    windows.window[-1].seq_id,
                    windows.window[0].key,  # key will be same anyway
                ).__dict__
            )
            yield self.env.timeout(self.model_runtime_s)
