import argparse
import json
import os
import time
from collections import defaultdict
from typing import List, Optional

import numpy as np
import pandas as pd
import psutil
import ray
import redis
from statsmodels.tsa.seasonal import STL, DecomposeResult

from ralf.core import Ralf
from ralf.operator import DEFAULT_STATE_CACHE_SIZE, Operator
from ralf.operators import LeftJoin, Source
from ralf.operators.source import KafkaSource
from ralf.policies import load_shedding_policy, processing_policy
from ralf.state import Record, Schema
from ralf.table import Table


@ray.remote
class RedisSource(Source):
    def __init__(
        self,
        topic: str,
        cache_size=DEFAULT_STATE_CACHE_SIZE,
        num_worker_threads: int = 1,
    ):
        schema = Schema(
            "key",
            {
                "key": str,
                "value": float,
                "timestamp": int,
                "send_time": float,
                "create_time": float,
            },
        )
        super().__init__(schema, cache_size, num_worker_threads=num_worker_threads)
        self.consumer = redis.Redis()

    def next(self) -> List[Record]:
        _, data = self.consumer.xreadgroup(
            "ralf-reader-group",
            f"reader-{self._shard_idx}",
            {"ralf": ">"},
            count=1,
            block=100 * 1000,
        )[0]
        record_id, payload = data[0]
        self.last_record = record_id
        self.consumer.xack("ralf", "ralf-reader-group", record_id)

        assert isinstance(payload, dict)
        record = Record(
            key=payload[b"key"].decode(),
            value=payload[b"value"].decode(),
            timestamp=int(payload[b"timestamp"]),
            send_time=float(payload[b"send_time"]),
            create_time=time.time(),
        )
        return [record]


@ray.remote
class FakeReader(Source):
    def __init__(
        self, num_keys, send_rate, timesteps=10000, cache_size=DEFAULT_STATE_CACHE_SIZE
    ):

        schema = Schema(
            "key",
            {
                "key": str,
                "value": float,
                "timestamp": int,
                "create_time": float,
                "send_time": float,
            },
        )

        super().__init__(schema, cache_size, num_worker_threads=1)
        self.num_keys = int(num_keys)
        self.send_rate = int(send_rate)
        self.timesteps = int(timesteps)
        self.ts = 0

    def next(self):
        try:
            if self.ts < self.timesteps * self.send_rate:
                records = []
                for key in range(self.num_keys):
                    t = time.time()
                    record = Record(
                        key=str(key),
                        value=1,
                        timestamp=self.ts,
                        create_time=t,
                        send_time=t,
                    )
                    records.append(record)
                self.ts += 1
                time.sleep(1 / self.send_rate)
                return records
            else:
                print("STOP ITERATION")
                raise StopIteration
        except Exception as e:
            print(e)


@ray.remote
class FileReader(Source):

    # Given a single key from the dataset, duplicates that stream to num_keys

    def __init__(
        self,
        num_keys,
        send_rate,
        filename,
        cache_size=DEFAULT_STATE_CACHE_SIZE,
    ):
        schema = Schema(
            "key",
            {
                "key": str,
                "value": float,
                "timestamp": int,
                "create_time": float,
                "send_time": float,
            },
        )

        super().__init__(schema, cache_size, num_worker_threads=1)

        print("Reading CSV", filename)

        df = pd.read_csv(filename)
        self.data = []
        for index, row in df.iterrows():
            self.data.append(row.to_dict())
        self.send_rate = send_rate
        self.num_keys = num_keys
        self.ts = 0

    def next(self):
        if self.ts < len(self.data):
            d = self.data[self.ts]
            records = []
            for k in range(self.num_keys):
                value = float(d["value"])
                timestamp = int(d["timestamps"])
                key = str(k)
                curr_time = time.time()
                records.append(
                    Record(
                        key=key,
                        value=value,
                        timestamp=timestamp,
                        create_time=curr_time,
                        send_time=curr_time,  # duplicated to be consistent with KafkaSource
                    )
                )
            self.ts += 1
            time.sleep(1 / self.send_rate)
            return records
        else:
            print("STOP ITERATION")
            raise StopIteration


@ray.remote
class STLTrainer(Operator):
    def __init__(
        self,
        seasonality,
        cache_size=DEFAULT_STATE_CACHE_SIZE,
        lazy=False,
        num_worker_threads=4,
        processing_policy=processing_policy.last_completed,
        load_shedding_policy=load_shedding_policy.later_complete_time,
    ):

        schema = Schema(
            "key",
            {
                "key": str,
                "trend": float,
                "seasonality": List[float],
                "send_time": float,
                "create_time": float,
                "complete_time": float,
                "timestamp": int,
            },
        )
        super().__init__(
            schema,
            cache_size,
            lazy,
            num_worker_threads,
            processing_policy,
            load_shedding_policy,
        )
        self.robust = True
        self.period = seasonality

    def on_record(self, record: Record) -> Record:
        values = [r.value for r in record.window]
        stl_result = STL(values, period=self.period, robust=self.robust).fit()

        return Record(
            key=record.key,
            trend=stl_result.trend[-1],
            seasonality=list(stl_result.seasonal[-(self.period + 1) : -1]),
            create_time=record.create_time,
            complete_time=time.time(),
            timestamp=record.timestamp,
        )


@ray.remote
class STLWindowTrain(Operator):
    def __init__(
        self,
        window_size: int,
        slide_size: int,
        period: int,
        cache_size=DEFAULT_STATE_CACHE_SIZE,
        lazy=False,
        num_worker_threads=4,
    ):
        # TODO: generate schema automatically from generics
        schema = Schema(
            "key",
            {
                "key": str,
                "trend": float,
                "seasonality": List[float],
                "send_time": float,
                "create_time": float,
                "complete_time": float,
                "timestamp": int,
            },
        )
        super().__init__(schema, cache_size, lazy, num_worker_threads)
        self.slide_size = slide_size
        self.window_size = window_size
        self.windows = {}
        self.period = period

    def on_record(self, record) -> Optional[Record]:
        try:
            key = getattr(record, self._table.schema.primary_key)
            window = self.windows.get(key, None)
            if window is None:
                window = []
                self.windows[key] = window

            self.windows[key].append(record)
            if len(self.windows[key]) >= self.window_size:
                window = self.windows[key]
                self.windows[key] = self.windows[key][self.slide_size :]

                # Fit STL
                stl_data = [r.value for r in window]

                # avoid silent error
                stl_result = STL(stl_data, period=self.period, robust=True).fit()
                return Record(
                    key=record.key,
                    trend=stl_result.trend[-1],
                    seasonality=list(stl_result.seasonal[-(self.period + 1) : -1]),
                    send_time=max([r.send_time for r in window]),
                    create_time=max([r.create_time for r in window]),
                    complete_time=time.time(),
                    timestamp=max([r.timestamp for r in window]),
                )
        except Exception as e:
            print(e)


@ray.remote
class STLInference(LeftJoin):
    def __init__(
        self, seasonality, cache_size=DEFAULT_STATE_CACHE_SIZE, lazy: bool = False
    ):
        schema = Schema(
            "key",
            {
                "key": str,
                "residual": float,
                "seasonality": float,
                "trend": float,
                "staleness": float,
                "version": int,  # model version
                "timestamp": int,
                "create_time": float,
                "complete_time": float,
            },
        )
        left_schema = Schema(
            "key",
            {
                "key": str,
                "value": float,
                "timestamp": int,
                "create_time": float,
            },
        )
        right_schema = Schema(
            "key",
            {"key": str, "model": DecomposeResult, "version": int, "timestamp": int},
        )

        super().__init__(schema, left_schema, right_schema, cache_size, lazy)

        self.period = seasonality

    def join(self, data_point: Record, model: Record) -> Record:

        # TODO: BE CAREFUL - changes based off timestamp units
        dt = int((data_point.timestamp - model.timestamp) / (60 * 60))

        last_trend = model.model.trend[-1]
        last_period = model.model.seasonal[-(self.period + 1) : -1]

        # assert model.model.seasonal[-1] == model.model.seasonal[-self.period], f"Last period: {model.model.seasonal}"
        seasonal = last_period[dt % len(last_period)]

        residual = data_point.value - last_trend - seasonal
        # print("inf", data_point.timestamp, model.timestamp, data_point.value, last_trend, seasonal, residual)

        return Record(
            key=data_point.key,
            residual=residual,
            seasonality=seasonal,
            trend=last_trend,
            timestamp=data_point.timestamp,
            create_time=data_point.create_time,
            complete_time=time.time(),
            staleness=dt,
            version=model.version,
        )


@ray.remote
class WriteOutput(Operator):
    def __init__(self, filename):
        # TODO: use a sink operator
        super().__init__(None, DEFAULT_STATE_CACHE_SIZE)
        self.writer = open(filename, "a")
        self.count = 0

    def on_record(self, record: Record):
        if str(record.key) != "0":  # only log for 1 key (avoid race)
            return

        now = time.time()
        output = dict(record.entries)
        output["write_time"] = now
        # print(output)

        # periodically gather system stats
        if self.count % 1000:
            output["system_stats"] = {
                "memory": psutil.virtual_memory().percent,
                "cpu": psutil.cpu_percent(interval=None),
            }

        self.writer.write(json.dumps(output) + "\n")
        self.count += 1


@ray.remote
class Logger(Operator):
    def __init__(self):
        # TODO: use a sink operator
        super().__init__(None, DEFAULT_STATE_CACHE_SIZE)
        self.send_times = []
        self.recv_times = []
        self.staleness = []
        self.residuals = defaultdict(list)

    def on_record(self, record: Record):
        now = time.time()
        self.send_times.append(record.create_time)
        self.recv_times.append(now)
        self.staleness.append(record.staleness)
        self.residuals[record.key].append(record.residual)

        if len(self.recv_times) % 1000 == 0:
            self.print_stats()
            # pickle.dump({
            #    "send_times": self.send_times,
            #    "recv_times": self.recv_times,
            #    "staleness": self.staleness,
            #    "residuals": self.residuals
            # }, open("info.pkl", "wb"))

    def print_stats(self):
        send_times = np.array(self.send_times)
        recv_times = np.array(self.recv_times)
        staleness = np.array(self.staleness)

        latencies = recv_times - send_times

        send_xput = (len(send_times) - 1) / (send_times[-1] - send_times[0])
        recv_xput = (len(recv_times) - 1) / (recv_times[-1] - recv_times[0])

        print("==========================")
        self.print_array("latency", latencies)
        self.print_array("send xput", send_xput)
        self.print_array("recv xput", recv_xput)
        self.print_array("staleness", staleness)

    def print_array(self, name, array):
        print(name)
        print(f" mean: {array.mean()}")
        print(f" std: {array.std()}")
        print(f" min: {array.min()}")
        print(f" 50%: {np.median(array)}")
        print(f" max: {array.max()}")
        print(f"samples: {array.size}")


# create synthetic data generating table
def from_synthetic(num_keys: int, send_rate: int):
    return Table([], FakeReader, num_keys, send_rate)


def from_file(num_keys: int, send_rate: int, f: str):
    return Table([], FileReader, num_keys, send_rate, f)


def from_kafka(topic: str):
    return Table([], KafkaSource, topic, num_replicas=4)


def from_redis(topic: str):
    return Table([], RedisSource, topic, num_replicas=2, num_worker_threads=1)


def write_metadata(args, ex_dir):
    # write experiment metadata
    metadata = {}
    metadata["num_keys"] = args.num_keys
    metadata["seasonality"] = args.seasonality
    metadata["window"] = args.window
    metadata["slide"] = args.slide
    metadata["send_rate"] = args.send_rate
    metadata["source_file"] = args.file
    open(os.path.join(ex_dir, "metadata.json"), "w").write(json.dumps(metadata))


def create_stl_pipeline(args):

    # create Ralf instance
    ralf_conn = Ralf(
        metric_dir=os.path.join(args.exp_dir, args.exp), log_wandb=True, exp_id=args.exp
    )

    # create pipeline
    if args.file is None:
        # source = from_synthetic(args.num_keys, args.send_rate)
        # source = from_kafka("ralf")
        source = from_redis("ralf")
    else:
        source = from_file(
            args.num_keys, args.send_rate, os.path.join(args.data_dir, args.file)
        )

    if args.per_key_slide_size_plan:
        with open(args.per_key_slide_size_plan) as f:
            per_key_slide_size_config = json.load(f)
            print(
                f"Using per key slide size config from {args.per_key_slide_size_plan}"
            )
    else:
        per_key_slide_size_config = None

    (
        source.window(
            args.window,
            args.slide,
            num_replicas=2,
            num_worker_threads=1,
            per_key_slide_size=per_key_slide_size_config,
        ).map(STLTrainer, args.seasonality, num_replicas=4, num_worker_threads=1)
    )

    # deploy
    ralf_conn.deploy(source, "source")

    return ralf_conn


def main():

    parser = argparse.ArgumentParser(description="Specify experiment config")
    parser.add_argument("--send-rate", type=int, default=100)
    parser.add_argument("--num-keys", type=int)
    parser.add_argument("--timesteps", type=int, default=10)
    # STL-specific
    parser.add_argument("--window", type=int, default=52)
    parser.add_argument("--slide", type=int, default=52)
    parser.add_argument("--seasonality", type=int)
    parser.add_argument("--per-key-slide-size-plan", type=str)

    # Experiment related
    parser.add_argument(
        "--data-dir",
        type=str,
        default="/Users/sarahwooders/repos/flink-feature-flow/datasets",
    )
    parser.add_argument(
        "--exp-dir",
        type=str,
        default="/Users/sarahwooders/repos/flink-feature-flow/RayServer/experiments",
    )
    parser.add_argument("--file", type=str, default=None)
    parser.add_argument("--exp", type=str)  # experiment id
    args = parser.parse_args()
    print(args)

    # create experiment directory
    os.makedirs(os.path.join(args.exp_dir, args.exp), exist_ok=True)
    write_metadata(args, os.path.join(args.exp_dir, args.exp))

    # create stl pipeline
    ralf_conn = create_stl_pipeline(args)
    ralf_conn.run()

    # snapshot stats
    run_duration = 6000
    snapshot_interval = 10  # 30
    start = time.time()
    while time.time() - start < run_duration:
        snapshot_time = ralf_conn.snapshot()
        remaining_time = snapshot_interval - snapshot_time
        if remaining_time < 0:
            print(
                f"snapshot interval is {snapshot_interval} but it took {snapshot_time} to perform it!"
            )
            time.sleep(0)
        else:
            print("writing snapshot", snapshot_time)
            time.sleep(remaining_time)
    print("TIME COMPLETE", run_duration)


if __name__ == "__main__":
    main()
