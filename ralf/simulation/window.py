from typing import Dict, DefaultDict, List
from collections import defaultdict

import simpy

from ralf.state import Record
from ralf.simulation.priority_queue import PerKeyPriorityQueue

KeyType = int


class WindowOperator:
    def __init__(
        self,
        env,
        window_size: int,
        slide_size: int,
        source_queue: simpy.Store,
        next_queues: Dict[KeyType, PerKeyPriorityQueue],
    ):
        self.env = env
        self.window_size = window_size
        self.slide_size = slide_size
        self.source_queue = source_queue
        self.windows: DefaultDict[int, List[float]] = defaultdict(list)
        self.next_queues = next_queues

        # I don't think we need this assert?
        # assert self.slide_size < self.window_size

        self.env.process(self.run())

    def run(self):
        while True:
            item = yield self.source_queue.get()

            self.windows[item.key].append(item)
            if len(self.windows[item.key]) == self.window_size:
                yield self.next_queues[item.key].put(
                    Record(
                        key=item.key,
                        window=self.windows[item.key],
                        processing_time=self.env.now,
                    )
                )
                self.windows[item.key] = self.windows[item.key][self.slide_size :]

            # NOTE(simon): edit this timeout for "window process time"
            yield self.env.timeout(0)
