from multiprocessing.sharedctypes import Value
from sqlite3 import Timestamp
import time
from collections import defaultdict
from dataclasses import dataclass, make_dataclass
from typing import List
import os
import redis

from ralf.v2.connectors.dict_connector import DictConnector
from ralf.v2.connectors.redis_connector import RedisConnector
from ralf.v2.connectors.sqlite3_connector import SQLConnector
from ralf.v2.record import Schema
from ralf.v2.table_state import TableState

from ralf.v2 import BaseTransform, RalfApplication, RalfConfig, Record
from ralf.v2.operator import OperatorConfig, RayOperatorConfig
from ralf.v2.utils import get_logger

logger = get_logger()

@dataclass
class SourceValue:
    key: str
    value: int
    timestamp: float

@dataclass
class LargeSourceValue:
    key: str
    value: str
    timestamp: float

LONG_TEXT_SIZE = 10 ** 6

#Number of records we're processing 
TEST_SIZES = [5,1000,1000000]
deploy_mode = "ray"
sizes_str = "_".join([str(s) for s in TEST_SIZES])
result_path = f"benchmark/results/size_{sizes_str}_{time.time()}.txt"

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
        self.num_keys = max(TEST_SIZES) #not sharding for now

    def on_event(self, _: Record) -> List[Record[SourceValue]]:

        if self.count >= self.total:
            print("completed iteration")
            raise StopIteration()

        self.count += 1
        key = str(self.count % self.num_keys)

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
class LargeFakeSource(BaseTransform):
    def __init__(self, total: int) -> None:
        self.count = 0
        self.total = total
        self.num_keys = max(TEST_SIZES) #not sharding for now

    def on_event(self, _: Record) -> List[Record[SourceValue]]:

        if self.count >= self.total:
            print("completed iteration")
            raise StopIteration()

        self.count += 1
        key = str(self.count % self.num_keys)

        # return list of records (wrapped around dataclass)
        return [
            Record(
                entry=LargeSourceValue(
                    key=key,
                    value=key*LONG_TEXT_SIZE,
                    timestamp=time.time(),
                ),
                shard_key=key,  # key to shard/query
            )
        ]

#write/update
class UpdateReadDelete(BaseTransform):
    def __init__(self, connector_type):
        self.connector_type = connector_type

    def on_event(self, record: Record) -> Record:
        self.getFF().table_state.update(record)
        self.getFF().table_state.point_query(record.entry.key)
        self.getFF().table_state.delete(record.entry.key)

        if int(record.entry.key) % 50000 == 0:
            logger.msg(f"Processed record: {record.entry.key}")
            log_and_get_metrics(self)
        return record
    
    def on_stop(self, record:Record) -> Record:
        latencies, throughputs = log_and_get_metrics(self)
        
        results = "-"*50 + f"\nCONNECTOR_MODE: {self.connector_type}\nLATENCIES (ms per action):\n"
        for k,v in latencies[self.connector_type].items():
            results += (f"{k}: {v}\n")
        results += "THROUGHPUTS (number of requests per second):\n"
        for k,v in throughputs[self.connector_type].items():
            results += (f"{k}: {v}\n")
        record_benchmark_results(results, result_path)

        return record

def log_and_get_metrics(transform):
    update_latency = transform.getFF().table_state.times["update"]/transform.getFF().table_state.counts["update"]
    read_latency = transform.getFF().table_state.times["point_query"]/transform.getFF().table_state.counts["point_query"]
    delete_latency = transform.getFF().table_state.times["delete"]/transform.getFF().table_state.counts["delete"]
    
    update_throughput = transform.getFF().table_state.counts["update"]/transform.getFF().table_state.times["update"]
    read_throughput = transform.getFF().table_state.counts["point_query"]/transform.getFF().table_state.times["point_query"]
    delete_throughput = transform.getFF().table_state.counts["delete"]/transform.getFF().table_state.times["delete"]

    logger.msg(f"AVERAGE UPDATE LATENCY: {update_latency * 1000} ms per update")
    logger.msg(f"AVERAGE READ LATENCY: {read_latency * 1000} ms per read")
    logger.msg(f"AVERAGE DELETE LATENCY: {delete_latency * 1000} ms per delete")

    logger.msg(f"AVERAGE UPDATE THROUGHPUTS: {update_throughput} updates per second")
    logger.msg(f"AVERAGE READ THROUGHPUTS: {read_throughput} reads per second")
    logger.msg(f"AVERAGE DELETE THROUGHPUTS: {delete_throughput} deletes per second")

    latencies[transform.connector_type]["update"] = update_latency * 1000
    latencies[transform.connector_type]["read"] = read_latency * 1000
    latencies[transform.connector_type]["delete"] = delete_latency * 1000
    
    throughputs[transform.connector_type]["update"] = update_throughput
    throughputs[transform.connector_type]["read"] = read_throughput
    throughputs[transform.connector_type]["delete"] = delete_throughput

    return latencies, throughputs

def record_benchmark_results(results, path):
    f = open(path, "a")
    f.write(results)
    f.close()

def flush_testing_env():
    if os.path.exists("key.db"):
        os.remove("key.db")
    r = redis.Redis()
    r.flushdb()

if __name__ == "__main__":
    for test_size in TEST_SIZES:
        record_benchmark_results(f"TIME: {time.localtime()}\nRECORD SIZE: {LONG_TEXT_SIZE}\nDEPLOY MODE: {deploy_mode}\nTEST SIZE: {test_size}\n\n", result_path)
        for connector_mode in existing_connectors:
            flush_testing_env()
            app = RalfApplication(RalfConfig(deploy_mode=deploy_mode))

            source_ff = app.source(
                LargeFakeSource(test_size),
                operator_config=OperatorConfig(
                    ray_config=RayOperatorConfig(num_replicas=1),
                ),
            )

            schema = Schema("key", {"key": str, "value": str, "timestamp": float})
            conn = existing_connectors[connector_mode]()
            table_state = TableState(schema, conn, LargeSourceValue)
            
            updateReadDelete_ff = source_ff.transform(
                UpdateReadDelete(connector_mode),
                operator_config=OperatorConfig(
                    ray_config=RayOperatorConfig(num_replicas=1),
                ),
                table_state = table_state
            )

            app.deploy()
            app.wait()
        record_benchmark_results("\n\n", result_path)