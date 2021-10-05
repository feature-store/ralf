import time
from typing import List, Optional

import pytest
import ray
from ray.util.queue import Empty, Queue

from ralf.core import Ralf
from ralf.operator import DEFAULT_STATE_CACHE_SIZE, Operator
from ralf.operators import Source
from ralf.policies import load_shedding_policy, processing_policy
from ralf.state import Record, Schema
from ralf.table import Table


@ray.remote
class CounterSource(Source):
    def __init__(self, send_up_to: int):
        self.count = 0
        self.send_up_to = send_up_to
        super().__init__(
            schema=Schema("key", {"key": str, "value": int}),
            cache_size=DEFAULT_STATE_CACHE_SIZE,
        )

    def next(self) -> Record:
        if self.count == 0:
            # Wait for downstream operators to come online.
            time.sleep(0.2)
        self.count += 1
        if self.count > self.send_up_to:
            raise StopIteration()
        return [Record(key="k", value=self.count)]


@ray.remote
class Sink(Operator):
    def __init__(self, result_queue: Queue):
        super().__init__(schema=None, cache_size=DEFAULT_STATE_CACHE_SIZE)
        self.q = result_queue

    def on_record(self, record: Record) -> Optional[Record]:
        self.q.put(record)
        return None


@ray.remote
class SlowNoop(Operator):
    def __init__(
        self,
        result_queue: Queue,
        processing_policy=processing_policy.fifo,
        load_shedding_policy=load_shedding_policy.always_process,
    ):
        super().__init__(
            schema=Schema("key", {"value": int}),
            cache_size=DEFAULT_STATE_CACHE_SIZE,
            num_worker_threads=1,
            processing_policy=processing_policy,
            load_shedding_policy=load_shedding_policy,
        )
        self.q = result_queue

    def on_record(self, record: Record) -> Optional[Record]:
        time.sleep(0.1)
        self.q.put(record)
        return record

    @classmethod
    def drop_smaller_values(cls, candidate: Record, current: Record):
        return candidate.value > current.value


def test_mapper():
    ralf = Ralf()

    queue = Queue()
    # send 1 to 100
    source_table = Table([], CounterSource, 100)
    sink = source_table.map(Sink, queue)

    ralf.deploy(source_table, "source")
    ralf.deploy(sink, "sink")

    ralf.run()

    records: List[Record] = [queue.get() for _ in range(100)]
    values = [record.value for record in records]
    # We have to sort it right now because it's not ordered correctly.
    assert sorted(values) == list(range(1, 101))


def test_processing_policy():
    ralf = Ralf()

    queue = Queue()
    # send 1 to 10
    source_table = Table([], CounterSource, 10)
    sink = source_table.map(SlowNoop, queue, processing_policy=processing_policy.lifo,)

    ralf.deploy(source_table, "source")
    ralf.deploy(sink, "sink")

    ralf.run()

    records: List[Record] = [queue.get() for _ in range(10)]
    values = [record.value for record in records]
    assert values == [1, 10, 9, 8, 7, 6, 5, 4, 3, 2]


def test_load_shedding_policy():
    ralf = Ralf()

    queue = Queue()
    # send 1 to 10
    source_table = Table([], CounterSource, 10)
    sink = source_table.map(
        SlowNoop,
        queue,
        processing_policy=processing_policy.lifo,
        load_shedding_policy=SlowNoop.drop_smaller_values,
    )

    ralf.deploy(source_table, "source")
    ralf.deploy(sink, "sink")

    ralf.run()

    records: List[Record] = [queue.get() for _ in range(2)]

    with pytest.raises(Empty):
        queue.get(timeout=1)

    values = [record.value for record in records]
    assert values == [1, 10]
