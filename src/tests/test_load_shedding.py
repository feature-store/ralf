from ralf import load_shedding_policy
from ralf.state import Record


def test_always_process():
    old_candidate = Record(a=1)
    current = Record(a=2)
    new_candidate = Record(a=3)

    assert load_shedding_policy.always_process(old_candidate, current)
    assert load_shedding_policy.always_process(new_candidate, current)


def test_newer_processing_time():
    old_candidate = Record(a=1)
    current = Record(a=2)
    new_candidate = Record(a=3)

    assert not load_shedding_policy.newer_processing_time(old_candidate, current)
    assert load_shedding_policy.newer_processing_time(new_candidate, current)


def test_custom():
    def newer_timestamp(candidate: Record, current: Record):
        return candidate.timestamp > current.timestamp

    old_candidate = Record(timestamp=3)
    current = Record(timestamp=2)
    new_candidate = Record(timestamp=1)

    assert newer_timestamp(old_candidate, current)
    assert not newer_timestamp(new_candidate, current)
