from ralf.policies import processing_policy
from ralf.record import Record


def test_fifo():
    first = Record(a=1)
    second = Record(a=2)

    assert processing_policy.fifo(first, second)
    assert not processing_policy.fifo(second, first)


def test_lifo():
    first = Record(a=1)
    second = Record(a=2)

    assert not processing_policy.lifo(first, second)
    assert processing_policy.lifo(second, first)


def test_custom():
    def custom_policy(first: Record, second: Record):
        return first.a < second.a

    a1 = Record(a=1)
    a2 = Record(a=2)
    a0 = Record(a=0)

    assert custom_policy(a1, a2)
    assert not custom_policy(a1, a0)
    assert not custom_policy(a2, a1)
    assert not custom_policy(a2, a0)
    assert custom_policy(a0, a1)
    assert custom_policy(a0, a2)
