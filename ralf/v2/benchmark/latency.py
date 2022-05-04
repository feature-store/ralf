import time
from collections import defaultdict
from dataclasses import dataclass
from typing import List

from nbformat import read
from ralf.v2.dict_connector import DictConnector
from ralf.v2.record import Schema
from ralf.v2.table_state import TableState

from ralf.v2 import BaseTransform, RalfApplication, RalfConfig, Record
from ralf.v2.operator import OperatorConfig, RayOperatorConfig


@dataclass
class SourceValue:
    key: str
    value: int
    timestamp: float

LOAD = 10

class FakeSource(BaseTransform):
    def __init__(self, total: int) -> None:
        self.count = 0
        self.total = total
        self.num_keys = 1000

    def on_event(self, _: Record) -> List[Record[SourceValue]]:

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

#write/update
class AddOne(BaseTransform):
    def on_event(self, record: Record) -> Record:
        record.entry.value += 1
        self.table_state.update(record)
        return record
    
    def on_stop(self, record:Record) -> Record:
        print("average update latency:" , self.table_state.times["update"]/self.table_state.counts["update"])
        return record

class ReadFromDict(BaseTransform):
    def __init__(self, total: int) -> None:
        self.total = total

    def on_event(self, record: Record) -> Record:
        self.table_state.point_query(record.entry.key)
        return record

    def on_stop(self, record:Record) -> Record:
        print("average query latency:" , self.table_state.times["point_query"]/self.table_state.counts["point_query"])
        return record

class DeleteFromDict(BaseTransform):
    def __init__(self, total: int) -> None:
        self.total = total

    def on_event(self, record: Record) -> Record:
        self.table_state.delete(record.entry.key)
        return record

    def on_stop(self, record:Record) -> Record:
        print("average delete latency:" , self.table_state.times["delete"]/self.table_state.counts["delete"])
        return record

if __name__ == "__main__":

    deploy_mode = "ray"
    connector = "dict"
    # deploy_mode = "local"
    app = RalfApplication(RalfConfig(deploy_mode=deploy_mode))

    source_ff = app.source(
        FakeSource(LOAD),
        operator_config=OperatorConfig(
            ray_config=RayOperatorConfig(num_replicas=1),
        ),
    )

    dict_schema = Schema("key", {"key": str, "value": int, "timestamp": float})
    dict_conn = DictConnector()
    table_state = TableState(dict_schema, dict_conn)

    addOne_ff = source_ff.transform(
        AddOne(),
        operator_config=OperatorConfig(
            ray_config=RayOperatorConfig(num_replicas=1),
        ),
        table_state=table_state
    )

    read_ff = addOne_ff.transform(
        ReadFromDict(LOAD),
        operator_config=OperatorConfig(
            ray_config=RayOperatorConfig(num_replicas=1),
        ), 
        table_state=table_state
    )

    delete_ff = read_ff.transform(
        DeleteFromDict(LOAD),
        operator_config=OperatorConfig(
            ray_config=RayOperatorConfig(num_replicas=1),
        ), 
        table_state=table_state
    )


    app.deploy()
    app.wait()
