import json
from collections import defaultdict
from typing import DefaultDict, Dict, List, Optional

import simpy

from ralf.simulation.priority_queue import PerKeyPriorityQueue
from ralf.state import Record

KeyType = int


class WindowOperator:
    def __init__(
        self,
        env,
        window_size: int,
        slide_size: int,
        source_queue: simpy.Store,
        next_queues: Dict[KeyType, PerKeyPriorityQueue],
        per_key_slide_size_path: Optional[str] = None,
    ):
        self.env = env
        self.window_size = window_size
        self.global_slide_size = slide_size
        if per_key_slide_size_path:
            self.per_key_slide_size_dict = json.load(open(per_key_slide_size_path))
        else:
            self.per_key_slide_size_dict = {}

        print("WINDOW", self.per_key_slide_size_dict)
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
                slide_size = (
                    self.per_key_slide_size_dict.get(str(item.key))
                    or self.global_slide_size
                )
                print("slide size", item.key, slide_size)
                self.windows[item.key] = self.windows[item.key][slide_size:]

            # NOTE(simon): edit this timeout for "window process time"
            yield self.env.timeout(0)
