import time
from collections import defaultdict
from dataclasses import dataclass
from typing import List
from ralf.v2.connectors.dict_connector import DictConnector
from ralf.v2.record import Schema
from ralf.v2.table_state import TableState

from ralf.v2 import BaseTransform, RalfApplication, RalfConfig, Record, Source
from ralf.v2.operator.operator import OperatorConfig, RayOperatorConfig


@dataclass
class SourceValue:
    key: str
    value: int
    timestamp: float


@dataclass
class SumValue:
    key: str
    value: int

class FakeSource(Source):
    def __init__(self, total: int) -> None:
        self.count = 0
        self.total = total
        self.num_keys = 10

    def next():

        if self.count >= self.total:
            print("completed iteration")
            raise StopIteration()

        self.count += 1
        key = str(self.count % self.num_keys)

        # sleep to slow down send rate
        time.sleep(1)

        # return list of records (wrapped around dataclass)
        return [
            Record(
                entry=SourceValue(
                    key=key,
                    value=self.count,
                    timestamp=time.time(),
                ),
                shard_key=key,  # key to shard/query
            )
        ]


class Sum(BaseTransform):
    def __init__(self):
        self.total = 0

    def on_event(self, record: Record) -> Record[SumValue]:
        self.total += record.entry.value
        print(f"Record {record.entry.key}, value {str(self.total)}")
        return Record(
            entry=SumValue(key=record.entry.key, value=self.total)
        )

class UpdateDict(BaseTransform):
    def __init__(self, table_state: TableState):
        self.table_state = table_state

    def on_event(self, record: Record) -> None:
        print("single update table", self.table_state.connector.tables)
        self.table_state.update(record)
        return None
    
class BatchUpdate(BaseTransform):
    def __init__(self, table_state:TableState, batch_size: int):
        self.batch_size = batch_size
        self.count = 0
        self.records = []
        self.table_state = table_state

    def on_event(self, record: Record) -> None:
        self.records.append(record)
        self.count += 1
        
        if self.count >= self.batch_size:
            self.count = 0
            for r in self.records:
                print(f"batch update, processing {r}")
                self.table_state.update(r)
            self.records = []
            print("batch table", self.table_state.connector.tables)
        
        return None

if __name__ == "__main__":

    deploy_mode = "ray"
    # deploy_mode = "local"
    app = RalfApplication(RalfConfig(deploy_mode=deploy_mode))

    source_ff = app.source(
        FakeSource(10),
        operator_config=OperatorConfig(
            ray_config=RayOperatorConfig(num_replicas=1),
        ),
    )

    sum_ff = source_ff.transform(
        Sum(),
        operator_config=OperatorConfig(
            ray_config=RayOperatorConfig(num_replicas=1),
        ),
    )

    dict_schema = Schema("key", {"key": str, "value": int})
    dict_schema_1 = Schema("key", {"key": str, "value": int})

    dict_conn = DictConnector()
    dict_conn_1 = DictConnector()

    dict_table_state = TableState(dict_schema, dict_conn, dataclass)
    batch_table_state = TableState(dict_schema_1, dict_conn_1, dataclass)

    update_ff = sum_ff.transform(
        UpdateDict(dict_table_state),
        operator_config=OperatorConfig(
            ray_config=RayOperatorConfig(num_replicas=1),
        ),
    )

    batch_updater = BatchUpdate(batch_table_state, 3)

    batch_update_ff = sum_ff.transform(
        batch_updater,
        operator_config=OperatorConfig(
            ray_config=RayOperatorConfig(num_replicas=1),
        ),
    )

    app.deploy()
    app.wait()
