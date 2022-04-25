import time
from collections import defaultdict
from dataclasses import dataclass, asdict
from typing import List
from ralf.v2.connectors.dict_connector import DictConnector
from ralf.v2.connectors.redis_connector import RedisConnector
from ralf.v2.connectors.sqlite3_connector import SQLConnector
import redis
from ralf.v2.record import Schema
from ralf.v2.table_state import TableState
from dataclasses_json import dataclass_json
import pytest
import os
import sqlite3

from ralf.v2 import BaseTransform, RalfApplication, RalfConfig, Record
from ralf.v2.operator import OperatorConfig, RayOperatorConfig

@dataclass_json
@dataclass
class SourceValue:
    key: str
    value: int
    timestamp: float

class FakeSource(BaseTransform):
    def __init__(self, total: int, table_state) -> None:
        self.count = 0
        self.total = total
        self.table_state = table_state
        self.num_keys = 10

    def on_event(self, _: Record) -> List[Record[SourceValue]]:

        if self.count >= self.total:
            print("completed iteration")
            raise StopIteration()

        self.count += 1
        key = str(self.count % self.num_keys)

        src_val = SourceValue(
                    key=key,
                    value=self.count,
                    timestamp=time.time(),
                )

        # return list of records (wrapped around dataclass)
        return [
            Record(
                entry=src_val,
                shard_key=key,  # key to shard/query
            )
        ]


class Sum(BaseTransform):
    def __init__(self, table_state):
        self.total = 0
        self.table_state = table_state

    def on_event(self, record: Record) -> Record[SourceValue]:  
        self.total += record.entry.value
        print(f"Record {record.entry.key}, value {str(self.total)}")
        src_val = SourceValue(key=record.entry.key, value=self.total, timestamp=record.entry.timestamp)
        return Record(
            entry=src_val
        )

class UpdateAndTest(BaseTransform):
    def __init__(self, table_state: TableState):
        self.table_state = table_state

    def on_event(self, record: Record) -> None:
        # print("single update table", self.table_state.connector.tables)
        self.table_state.update(record)
        # assert record == self.table_state.point_query(record.entry.key)
        self.table_state.delete(record.entry.key)
        # assert None == self.table_state.point_query(record.entry.key)
        return None
    
class BatchUpdateAndTest(BaseTransform):
    def __init__(self, table_state: TableState ,batch_size: int):
        self.batch_size = batch_size
        self.count = 0
        self.records = []
        self.all_records = []
        self.table_state = table_state

    def on_event(self, record: Record) -> None:
        self.records.append(record)
        self.all_records.append(record)
        self.count += 1
        
        if self.count >= self.batch_size:
            self.count = 0
            for r in self.records:
                print(f"batch update, processing {r}")
                self.table_state.update(r)
            for r in self.records:
                assert self.table_state.point_query(r.entry.key) == r
            self.records = []
        try:
            for r in self.table_state.bulk_query():
                if r not in self.all_records:
                    assert False
        except Exception as e:
            pytest.fail(e, pytrace=True)
        return None

def clear_testing_files():
    if os.path.exists('key.db'):
        os.remove("key.db")
    if os.path.exists('key2.db'):
        os.remove("key2.db") 

@pytest.mark.parametrize("connector_name", ["redis", "sqlite", "dict"])
def test_connectors(connector_name):
    clear_testing_files()

    deploy_mode = "ray"
    # deploy_mode = "local"
    app = RalfApplication(RalfConfig(deploy_mode=deploy_mode))

    dict_schema = Schema("key", {"timestamp": float, "key": str, "value": int})
    dict_schema_1 = Schema("key", {"timestamp": float, "key": str, "value": int})

    conn_1, conn_2 = None, None

    if connector_name == "redis":
        conn_1 = RedisConnector(host='127.0.0.1', port='6379', password='')
        conn_2 = RedisConnector(host='127.0.0.1', port='6379', password='')
    elif connector_name == "sqlite":
        conn_1 = SQLConnector(dbname="key.db")
        conn_2 = SQLConnector(dbname="key2.db")
    else:
        conn_1 = DictConnector()
        conn_2 = DictConnector()

    redis_table_state = TableState(dict_schema, conn_1, SourceValue)
    batch_table_state = TableState(dict_schema_1, conn_2, SourceValue)

    source_ff = app.source(
        FakeSource(10, redis_table_state),
        operator_config=OperatorConfig(
            ray_config=RayOperatorConfig(num_replicas=1),
        ),
    )

    sum_ff = source_ff.transform(
        Sum(redis_table_state),
        operator_config=OperatorConfig(
            ray_config=RayOperatorConfig(num_replicas=1),
        ),
    )

    update_ff = sum_ff.transform(
        UpdateAndTest(redis_table_state),
        operator_config=OperatorConfig(
            ray_config=RayOperatorConfig(num_replicas=1),
        ),
    )

    batch_update_ff = sum_ff.transform(
        BatchUpdateAndTest(batch_table_state, 3),
        operator_config=OperatorConfig(
            ray_config=RayOperatorConfig(num_replicas=1),
        ),
    )

    app.deploy()
    app.wait()
