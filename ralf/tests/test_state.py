import pytest

from ralf.state import Record, Schema, TableState


def test_record():
    r = Record(a="a", b="b")

    # getattr should work
    assert r.a == "a"
    assert r.b == "b"


def test_schema():
    schema = Schema(primary_key="key", columns={"a": str, "b": int})

    schema.validate_record(Record(key=1, a="a", b=1))
    with pytest.raises(AssertionError):
        schema.validate_record(Record(a="a"))


def test_table_state():
    state = TableState(Schema(primary_key="key", columns={"a": str}))
    # test update
    state.update(Record(key=1, a="a"))
    with pytest.raises(AttributeError):
        state.update(Record(no_primary_key=2))

    # test point query
    assert state.point_query(key=1) == Record(a="a", key=1)
    with pytest.raises(KeyError, match="not found"):
        state.point_query(1000)

    # test bulk query
    state.update(Record(key=2, a="b"))
    assert state.bulk_query() == [
        Record(key=1, a="a"),
        Record(key=2, a="b"),
    ]

    # test state update
    state.update(Record(key=2, a="c"))
    assert state.point_query(2).a == "c"

    # test deletion
    state.delete(key=2)
    with pytest.raises(KeyError, match="not found"):
        state.point_query(2)
