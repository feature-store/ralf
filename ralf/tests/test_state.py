import sqlite3

import pytest
import redis

from ralf.state import Record, Schema
from ralf.tables.dict_connector import DictConnector
from ralf.tables.redis_connector import RedisConnector
from ralf.tables.sqlite_connector import SQLiteConnector
from ralf.tables.table_state import TableState


def test_record():
    r = Record(a="a", b="b")

    # getattr should work
    assert r.a == "a"
    assert r.b == "b"


def test_schema():
    schema = Schema(primary_key="key", columns={"key": int, "a": str, "b": int})

    schema.validate_record(Record(key=1, a="a", b=1))
    with pytest.raises(AssertionError):
        schema.validate_record(Record(a="a"))


dict_connector = DictConnector()
sqlite_connector = SQLiteConnector(sqlite3.connect("test.db"))
redis_connector = RedisConnector(redis.Redis())
connectors = [dict_connector, sqlite_connector, redis_connector]


@pytest.mark.parametrize("connector", connectors)
def test_table_state(connector):
    state = TableState(
        Schema(primary_key="key", columns={"key": int, "a": str}), connector
    )
    # test update
    state.update(Record(key=1, a="a"))
    with pytest.raises(AssertionError):  # what kind of error should be raised?
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
    assert state.bulk_query() == [
        Record(key=1, a="a"),
    ]

    assert state.debug_state() == {
        "num_updates": 3,
        "num_deletes": 1,
        "num_records": 1,
    }
