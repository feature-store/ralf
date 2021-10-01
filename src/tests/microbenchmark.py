from typing import Optional, List
from pprint import pprint
import asyncio
import time

import numpy as np

import ray
from ray.util.queue import Queue, Empty

from ralf.operator import Operator, DEFAULT_STATE_CACHE_SIZE
from ralf.operators import Source
from ralf.table import Table
from ralf.core import Ralf
from ralf.state import Schema, Record
from ralf import processing_policy, load_shedding_policy


@ray.remote(num_cpus=0)
class Reporter:
    def __init__(self):
        self.state = {}
        self.waiters = {}

    def put(self, key, value):
        self.state[key] = value
        if key in self.waiters:
            self.waiters[key].set()
            del self.waiters[key]

    async def get(self, key):
        if key not in self.waiters:
            event = asyncio.Event()
            self.waiters[key] = event
            await event.wait()

        return self.state[key]


@ray.remote
class BenchSource(Source):
    def __init__(self, send_up_to: int):
        self.count = 0
        self.send_up_to = send_up_to

        self.start_time_ns = 0
        self.last_time_ns = 0
        self.end_time_ns = 0
        self.duration_ns = []

        self.reporter = ray.get_actor("reporter")

        super().__init__(
            schema=Schema("key", {"key": str, "value": int}),
            cache_size=0,
            num_worker_threads=1,
        )

    def next(self) -> Record:
        if self.count == 0:
            self.start_time_ns = time.perf_counter_ns()

        if self.last_time_ns != 0:
            now = time.perf_counter_ns()
            self.duration_ns.append(now - self.last_time_ns)
        self.last_time_ns = time.perf_counter_ns()

        self.count += 1

        if self.count > self.send_up_to:
            self.end_time_ns = time.perf_counter_ns()
            self.reporter.put.remote(
                "source",
                {
                    "duration_percentiles_percent": [1, 25, 50, 80, 90, 95, 99],
                    "duration_percentiles_ns": np.percentile(
                        self.duration_ns, [1, 25, 50, 80, 90, 95, 99]
                    ),
                    "total_count": self.count,
                    "total_duration_ns": self.end_time_ns - self.start_time_ns,
                    "throughput_records_per_sec": self.count
                    / ((self.end_time_ns - self.start_time_ns) / 1e9),
                },
            )
            raise StopIteration()

        if self.count == self.send_up_to:
            return [Record(key="k", value="last")]

        return [Record(key="k", value=self.count)]


@ray.remote
class Mapper(Operator):
    def __init__(self):
        self.start_time_ns = 0
        self.end_time_ns = 0
        self.count = 0
        self.reporter = ray.get_actor("reporter")

        super().__init__(
            schema=Schema("key", {"key": str, "value": int}),
            cache_size=0,
            num_worker_threads=1,
        )

    def on_record(self, record: Record) -> Optional[Record]:
        if self.start_time_ns == 0:
            self.start_time_ns = time.perf_counter_ns()

        self.count += 1

        if record.value == "last":
            self.end_time_ns = time.perf_counter_ns()
            self.reporter.put.remote(
                "mapper_throughput_qps",
                {
                    "total_count": self.count,
                    "duration_ns": self.end_time_ns - self.start_time_ns,
                    "throughput_qps": self.count
                    / ((self.end_time_ns - self.start_time_ns) / 1e9),
                },
            )
        # return record


@ray.remote
class Blackhole(Operator):
    def __init__(self):
        super().__init__(schema=Schema("key", {"key": str, "value": int}), cache_size=0)

    def on_record(self, _record: Record) -> Optional[Record]:
        return None


def bench_source():
    ralf = Ralf()

    reporter = Reporter.options(name="reporter").remote()

    source_table = Table([], BenchSource, int(2e4))
    ralf.deploy(source_table, "source")
    ralf.run()

    benchmark_result = ray.get(reporter.get.remote("source"))
    pprint(benchmark_result)


def bench_one_sink():
    ralf = Ralf()

    reporter = Reporter.options(name="reporter").remote()

    source_table = Table([], BenchSource, int(2e4))
    sink = source_table.map(Blackhole)
    ralf.deploy(source_table, "source")
    ralf.deploy(sink, "sink")
    ralf.run()

    benchmark_result = ray.get(reporter.get.remote("source"))
    pprint(benchmark_result)


def bench_mapper_throughput():
    ralf = Ralf()

    reporter = Reporter.options(name="reporter").remote()

    source_table = Table([], BenchSource, int(1e3), num_replicas=4)
    mapper = source_table.map(Mapper)
    sink = mapper.map(Blackhole)
    ralf.deploy(source_table, "source")
    ralf.deploy(mapper, "mapper")
    ralf.deploy(sink, "sink")
    ralf.run()

    benchmark_result = ray.get(reporter.get.remote("mapper_throughput_qps"))
    pprint(benchmark_result)


if __name__ == "__main__":
    # bench_source()
    # bench_one_sink()
    bench_mapper_throughput()
