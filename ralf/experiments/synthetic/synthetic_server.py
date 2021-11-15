import time
from typing import List, Optional

import pytest
import ray
from ray.util.queue import Empty, Queue

from ralf.core import Ralf
from ralf.operator import DEFAULT_STATE_CACHE_SIZE, Operator
from ralf.operators.source import Source
from ralf.policies import load_shedding_policy, processing_policy
from ralf.state import Record, Schema
from ralf.table import Table
from ralf.client import RalfClient

@ray.remote
class CounterSource(Source):
    def __init__(self, send_up_to: int, num_keys: int, send_rate: float):
        self.count = 0
        self.send_up_to = send_up_to
        self.num_keys = num_keys
        self.send_rate = send_rate
        self.num_worker_threads = 4

        super().__init__(
            schema=Schema(
                "key", {
                    "key": str, 
                    "value": int,
                    "create_time": float
                }),
            cache_size=DEFAULT_STATE_CACHE_SIZE,
        )

    def next(self) -> Record:
        time.sleep(1 / self.send_rate)
        if self.count == 0:
            # Wait for downstream operators to come online.
            time.sleep(0.2)
        self.count += 1
        if self.count > self.send_up_to:
            raise StopIteration()
        return [Record(
            key=str(self.count % self.num_keys), 
            value=self.count,
            create_time=time.time()
        )]


# @ray.remote
# class Sink(Operator):
#     def __init__(self, result_queue: Queue):
#         super().__init__(schema=None, cache_size=DEFAULT_STATE_CACHE_SIZE)
#         self.q = result_queue

#     def on_record(self, record: Record) -> Optional[Record]:
#         self.q.put(record)
#         return record

@ray.remote
class SlowIdentity(Operator):
    def __init__(
        self,
        result_queue: Queue,
        processing_time: float,
        processing_policy=processing_policy.fifo,
        load_shedding_policy=load_shedding_policy.always_process,
        lazy=False
    ):
        super().__init__(
            schema=Schema("key", {"value": int}),
            cache_size=DEFAULT_STATE_CACHE_SIZE,
            num_worker_threads=4,
            processing_policy=processing_policy,
            load_shedding_policy=load_shedding_policy,
            lazy=lazy
        )
        self.q = result_queue
        self.processing_time = processing_time

    def on_record(self, record: Record) -> Optional[Record]:
        time.sleep(self.processing_time)
        record = Record(
            key=record.key,
            value=record.value,
            create_time=time.time(),
        )
        self.q.put(record)
        return record


def create_synthetic_pipeline(queue):
    ralf = Ralf()
    
    # create pipeline
    # source_table = Table([], CounterSource, 1000000, 3, 1000)
    source_table = Table([], CounterSource, 1000000, 3000, 100000) #send_up_to, num_keys, send_rate

    sink = source_table.map(
        SlowIdentity,
        queue,
        0.1,
        lazy=True
    ).as_queryable("sink")
    
    # deploy
    ralf.deploy(source_table, "source")
    ralf.deploy(sink, "sink")

    return ralf


def main():
    # create synthetic pipeline
    queue = Queue()
    ralf = create_synthetic_pipeline(queue)
    ralf.run()

    # snapshot stats
    run_duration = 20
    snapshot_interval = 2
    start = time.time()
    while time.time() - start < run_duration:
        snapshot_time = ralf.snapshot()
        remaining_time = snapshot_interval - snapshot_time
        if remaining_time < 0:
            print(
                f"snapshot interval is {snapshot_interval} but it took {snapshot_time} to perform it!"
            )
            time.sleep(0)
        else:
            print("writing snapshot", snapshot_time)
            # records: List[Record] = [queue.get() for _ in range(2)]
            # print([f"{record}: {record.latest_query_time - record.create_time}" for record in records])
            time.sleep(remaining_time)


if __name__ == "__main__":
    main()