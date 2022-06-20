import time
from collections import defaultdict
from dataclasses import dataclass, make_dataclass
from typing import List

from ralf.v2.connectors.dict_connector import DictConnector
from ralf.v2.connectors.redis_connector import RedisConnector
from ralf.v2.connectors.sqlite3_connector import SQLConnector
from ralf.v2.record import Schema
from ralf.v2.table_state import TableState

from ralf.v2 import BaseTransform, RalfApplication, RalfConfig, Record
from ralf.v2.operator import OperatorConfig, RayOperatorConfig
from ralf.v2.utils import get_logger

import redis
import os

IntValue = make_dataclass("IntValue", ["value"])

logger = get_logger()

@dataclass
class SumValue:
    key: str
    value: int

@dataclass
class SourceValue:
    key: str
    value: int
    timestamp: float

#Number of records we're processing 
TEST_SIZE = 50

latencies = defaultdict(dict)
throughputs = defaultdict(dict)

#functions that create the connectors depending on specified connector mode
existing_connectors = {
    "dict": DictConnector, 
    "redis": lambda: RedisConnector(host='127.0.0.1', port='6379', password=''), 
    "sqlite": lambda: SQLConnector(dbname="key.db")
}

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
class UpdateReadDelete(BaseTransform):
    def __init__(self, connector_type, table_state):
        self.connector_type = connector_type
        self.table_state = table_state

    def on_event(self, record: Record) -> Record:
        record.entry.value += 1

        self.table_state.update(record)
        self.table_state.point_query(record.entry.key)
        self.table_state.delete(record.entry.key)

        return record
    
    def on_stop(self, record:Record) -> Record:
        update_latency = self.table_state.times["update"]/self.table_state.counts["update"]
        read_latency = self.table_state.times["point_query"]/self.table_state.counts["point_query"]
        delete_latency = self.table_state.times["delete"]/self.table_state.counts["delete"]
        
        update_throughput = self.table_state.counts["update"]/self.table_state.times["update"]
        read_throughput = self.table_state.counts["point_query"]/self.table_state.times["point_query"]
        delete_throughput = self.table_state.counts["delete"]/self.table_state.times["delete"]

        logger.msg(f"AVERAGE UPDATE LATENCY: {update_latency * 1000} ms per update")
        logger.msg(f"AVERAGE READ LATENCY: {read_latency * 1000} ms per read")
        logger.msg(f"AVERAGE DELETE LATENCY: {delete_latency * 1000} ms per delete")

        logger.msg(f"AVERAGE UPDATE LATENCY: {update_throughput} updates per second")
        logger.msg(f"AVERAGE READ LATENCY: {read_throughput} reads per second")
        logger.msg(f"AVERAGE DELETE LATENCY: {delete_throughput} deletes per second")

        latencies[self.connector_type]["update"] = update_latency
        latencies[self.connector_type]["read"] = read_latency
        latencies[self.connector_type]["delete"] = delete_latency
        
        throughputs[self.connector_type]["update"] = update_throughput
        throughputs[self.connector_type]["read"] = read_throughput
        throughputs[self.connector_type]["delete"] = delete_throughput
        
        f = open(f"benchmark/results/{time.time()}.txt", "a")
        results = "mode: ray\nconnector_mode: " +  self.connector_type + "\nlatencies:\n"
        for k,v in latencies.items():
            results += (f"{k}: {v}\n")
        results += "throughputs:\n"
        for k,v in throughputs.items():
            results += (f"{k}: {v}\n")
        f.write(results)
        f.close()

        return record


def flush_testing_env():
    if os.path.exists("key.db"):
        os.remove("key.db")
    r = redis.Redis()
    r.flushdb()

if __name__ == "__main__":

    flush_testing_env()
    deploy_mode = "ray"
    connector_mode = "dict"
    # deploy_mode = "local"
    app = RalfApplication(RalfConfig(deploy_mode=deploy_mode))

    source_ff = app.source(
        FakeSource(TEST_SIZE),
        operator_config=OperatorConfig(
            ray_config=RayOperatorConfig(num_replicas=1),
        ),
    )

    table_States = []
    for _ in range(3):
        dict_schema = Schema("key", {"key": str, "value": int, "timestamp": float})
        dict_conn = existing_connectors[connector_mode]()
        table_States.append(TableState(dict_schema, dict_conn, SourceValue))
    
    updateReadDelete_ff = source_ff.transform(
        UpdateReadDelete(connector_mode, table_States[0]),
        operator_config=OperatorConfig(
            ray_config=RayOperatorConfig(num_replicas=1),
        ),
        table_state=table_States[0]
    )

    app.deploy()
    app.wait()
