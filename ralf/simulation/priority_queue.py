import bisect
from functools import total_ordering

import simpy


@total_ordering
class _SortableRecordWrapper:
    def __init__(self, record, processing_policy):
        self.record = record
        self.processing_policy = processing_policy

    def __eq__(self, other) -> bool:
        return (
            self.record == other.record
            and self.processing_policy == other.processing_policy
        )

    def __lt__(self, other):
        return self.processing_policy(self.record, other.record)


class PerKeyPriorityQueue(simpy.Store):
    def __init__(
        self, env: simpy.Environment, processing_policy=None, load_shedding_policy=None,
    ):
        self.processing_policy = processing_policy
        self.load_shedding_policy = load_shedding_policy

        self.last_value = None

        super().__init__(env)

    def _do_put(self, event):
        # Insert sorted.
        bisect.insort(
            self.items, _SortableRecordWrapper(event.item, self.processing_policy)
        )
        event.succeed()

    def _do_get(self, event):
        if self.items:
            value = self.items.pop(0).record

            should_process = True
            if self.load_shedding_policy and self.last_value:
                should_process = self.load_shedding_policy(
                    candidate=value, current=self.last_value
                )
                if not should_process:
                    print("dropping ", value)

            if should_process:
                event.succeed(value)
                self.last_value = value
        return None
