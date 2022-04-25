import time
from collections import defaultdict
from dataclasses import dataclass, asdict
from typing import List
from ralf.v2.connectors.dict_connector import DictConnector
from ralf.v2.connectors.redis_connector import RedisConnector
import redis
from ralf.v2.record import Schema
from ralf.v2.table_state import TableState
from dataclasses_json import dataclass_json

from ralf.v2 import BaseTransform, RalfApplication, RalfConfig, Record
from ralf.v2.operator import OperatorConfig, RayOperatorConfig

@dataclass_json
@dataclass
class SourceValue:
    key: str
    value: int
    timestamp: float

# @dataclass
# class SumValue:
#     key: str
#     value: int
# @dataclass_json
# @dataclass
# class SourceValue:
#         self.key = key
#         self.value = value
#         self.timestamp = timestamp

# class SourceValue:
#     def __init__(self, key:str, value: int, timestamp: float):
#         self.key = key
#         self.value = value
#         self.timestamp = timestamp
#     def is_dataclass(self):
#         return True


class FakeSource(BaseTransform):
    def __init__(self, total: int) -> None:
        self.count = 0
        self.total = total
        self.num_keys = 10

    def on_event(self, _: Record) -> List[Record[SourceValue]]:

        if self.count >= self.total:
            print("completed iteration")
            raise StopIteration()

        self.count += 1
        key = str(self.count % self.num_keys)

        # sleep to slow down send rate
        time.sleep(1)

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
    def __init__(self):
        self.total = 0

    def on_event(self, record: Record) -> Record[SourceValue]:  
        self.total += record.entry.value
        print(f"Record {record.entry.key}, value {str(self.total)}")
        src_val = SourceValue(key=record.entry.key, value=self.total, timestamp=record.entry.timestamp)
        return Record(
            entry=src_val
        )

class UpdateDict(BaseTransform):
    def __init__(self, table_state: TableState):
        self.table_state = table_state

    def on_event(self, record: Record) -> None:
        # print("single update table", self.table_state.connector.tables)
        self.table_state.update(record)
        print("query result!: ", self.table_state.point_query(record.entry.key))
        return None
    
class BatchUpdate(BaseTransform):
    def __init__(self, table_state: TableState ,batch_size: int):
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
            # print("batch table", self.table_state.connector.tables)
        
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

    dict_schema = Schema("key", {"timestamp": float, "key": str, "value": int})
    dict_schema_1 = Schema("key", {"timestamp": float, "key": str, "value": int})

    redis_conn = RedisConnector(host='127.0.0.1', port='6379', password='')
    redis_conn_1 = RedisConnector(host='127.0.0.1', port='6379', password='')

    dict_table_state = TableState(dict_schema, redis_conn, SourceValue)
    batch_table_state = TableState(dict_schema_1, redis_conn_1, SourceValue)

    update_ff = sum_ff.transform(
        UpdateDict(dict_table_state),
        operator_config=OperatorConfig(
            ray_config=RayOperatorConfig(num_replicas=1),
        ),
    )

    batch_update_ff = sum_ff.transform(
        BatchUpdate(batch_table_state, 3),
        operator_config=OperatorConfig(
            ray_config=RayOperatorConfig(num_replicas=1),
        ),
    )

    app.deploy()
    app.wait()
