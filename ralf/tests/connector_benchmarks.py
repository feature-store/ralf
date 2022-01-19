import random
import sqlite3
from string import ascii_lowercase

import redis

from ralf.state import Record, Schema
from ralf.tables.dict_connector import DictConnector
from ralf.tables.redis_connector import RedisConnector
from ralf.tables.sqlite_connector import SQLiteConnector
from ralf.tables.table_state import TableState

dict_connector = DictConnector()
sqlite_connector = SQLiteConnector(sqlite3.connect("test.db"))
redis_connector = RedisConnector(redis.Redis())


def benchmark_update(connector):
    state = TableState(
        Schema(primary_key="key", columns={"key": int, "a": str}), connector
    )
    for i in range(1000):
        state.update(
            Record(key=random.randint(1, 1000), a=random.choice(ascii_lowercase))
        )


def test_update_dict(benchmark):
    benchmark(benchmark_update, dict_connector)


def test_update_redis(benchmark):
    benchmark(benchmark_update, redis_connector)


def test_update_sqlite(benchmark):
    benchmark(benchmark_update, sqlite_connector)
