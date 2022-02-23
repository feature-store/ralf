from dataclasses import dataclass

from ralf.v2.record import Record
from ralf.v2.scheduler import KeyCount, LeastUpdate


@dataclass
class SourceValue:
    key: int
    user_id: int
    rating: int
    timestamp: int


num_events = 50
events = [i for i in range(num_events)]
records = [Record(SourceValue(i // 5, i + 1, i - 1, 10)) for i in events]


def test_scheduler():
    l_u = LeastUpdate()
    # events has 10 records per key
    for i in range(num_events):
        l_u.push_event(records[i])
    comparision_set = set([i for i in range(10)])
    count = 0
    seen_set = set()
    for i in range(num_events):
        if count == 10:
            assert seen_set == comparision_set
            seen_set = set()
            count = 0
        record = l_u.pop_event()
        seen_set.add(record.entry.key)
        count += 1


def test_kc_comparisions():
    kc_one = KeyCount(4, 0, records[0])
    kc_two = KeyCount(4, 1, records[1])

    assert kc_one < kc_two
    assert kc_one > kc_two


test_scheduler()
