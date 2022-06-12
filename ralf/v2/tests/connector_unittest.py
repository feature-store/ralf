import time
from dataclasses import make_dataclass, dataclass
from dataclasses_json import dataclass_json

import numpy as np
import pytest
import simpy

from ralf.v2.connectors.dict_connector import DictConnector
from ralf.v2.connectors.redis_connector import RedisConnector
from ralf.v2.connectors.sqlite3_connector import SQLConnector

from ralf.v2 import LIFO, BaseTransform, RalfApplication, RalfConfig, Record
from ralf.v2.operator import OperatorConfig, RayOperatorConfig, SimpyOperatorConfig
from ralf.v2.utils import get_logger
from ralf.v2.table_state import TableState
from ralf.v2.record import Schema

import os
import redis

IntValue = make_dataclass("IntValue", ["value"])


logger = get_logger()

@dataclass_json
@dataclass
class SourceValue:
    key: str
    value: int
    timestamp: float

class CounterSource(BaseTransform):
    def __init__(self, up_to: int) -> None:
        self.count = 0
        self.up_to = up_to

    def on_event(self, record: Record) -> Record[IntValue]:
        self.count += 1
        if self.count >= self.up_to:
            logger.msg("self.count reached to self.up_to, sending StopIteration")
            raise StopIteration()
        return Record(
            id_=record.id_,
            entry=IntValue(value=self.count),
            shard_key=str(self.count % 10),
        )


class SumUpdateTableState(BaseTransform):
    def __init__(self, table_state) -> None:
        self.state = 0
        self.history = []
        self.table_state = table_state

    def on_event(self, record: Record[IntValue]):
        self.history.append(record.entry.value)
        self.state += record.entry.value
        modified_val = SourceValue(
            key=str(record.id_),
            value=record.entry.value,
            timestamp=time.time()
        )
        modified_record = Record(
            entry=modified_val,
            shard_key=str(record.id_)
        )
        self.table_state.update(modified_record)
        assert modified_record == self.table_state.point_query(modified_record.entry.key)
        self.table_state.delete(modified_record.entry.key)
        try:
            self.table_state.point_query(modified_record.entry.key)
        except Exception as e:
            assert type(e) == KeyError
        time.sleep(0.1)
        return None

def flush_testing_env():
    if os.path.exists("key.db"):
        os.remove("key.db")
    r = redis.Redis()
    r.flushdb()

@pytest.mark.parametrize("connector_name", ["dict", "redis", "sqlite"])
@pytest.mark.parametrize("deploy_mode", ["ray", "local"])
def test_connectors_simple(connector_name, deploy_mode):
    flush_testing_env()

    app = RalfApplication(RalfConfig(deploy_mode=deploy_mode))

    dict_schema = Schema("key", {"timestamp": float, "key": str, "value": int})

    if connector_name == "redis":
        conn = RedisConnector(host='127.0.0.1', port='6379', password='')
    elif connector_name == "sqlite":
        conn = SQLConnector(dbname="key.db")
    else:
        conn = DictConnector()

    table_state = TableState(dict_schema, conn, SourceValue)

    app.source(CounterSource(10)).transform(
        SumUpdateTableState(table_state),
        LIFO(),
        operator_config=OperatorConfig(ray_config=RayOperatorConfig(num_replicas=2)),
    )
    app.deploy()
    app.wait()

    flush_testing_env()