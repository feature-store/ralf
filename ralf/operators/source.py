import asyncio
import time
import traceback
from abc import ABC, abstractmethod
from typing import List

import ray
from ray.actor import ActorHandle

from ralf.operator import DEFAULT_STATE_CACHE_SIZE, Operator
from ralf.state import Record, Schema


class Source(Operator, ABC):
    def __init__(self, schema: Schema, cache_size=DEFAULT_STATE_CACHE_SIZE, **kwargs):
        super().__init__(schema, cache_size, lazy=False, **kwargs)

    async def _next(self):
        """Runs the source by iteratively invoking next().

        Schedules the subsequent run of next() via Ray so that the actor
        doesn't block and can serve queries.
        """
        while True:
            try:
                records = self.next()
            except Exception as e:
                if not isinstance(e, StopIteration):
                    traceback.print_exc()
                return
            # TODO(peter): optimize by adding batch send.
            for record in records:
                # assert isinstance(record, Record) # make sure record is a Record object
                print("sending", record)
                self.send(record)
            # Yield the coroutine so it can be queried.
            await asyncio.sleep(0)

    def on_record(self, record: Record):
        pass

    @abstractmethod
    def next(self) -> List[Record]:
        """Runs an iteration that returns a list of records.

        Records are automatically sent to downstream operators.
        Should raise an exception when the source runs out of input.
        """

    def query(self, source_handle: ActorHandle, key: str):
        raise NotImplementedError


SourceOperator = Source


@ray.remote
class KafkaSource(Source):
    def __init__(self, topic: str, cache_size=DEFAULT_STATE_CACHE_SIZE):
        import msgpack
        from kafka import KafkaConsumer

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
        super().__init__(schema, cache_size)
        self.consumer = KafkaConsumer(
            topic, bootstrap_servers="localhost:9092", value_deserializer=msgpack.loads
        )

    def next(self) -> List[Record]:

        event = next(self.consumer)
        assert isinstance(event.value, dict)
        record = Record(
            key=str(event.value["key"]),
            value=event.value["value"],
            timestamp=int(event.value["timestamp"]),
            send_time=event.value["send_time"],
            create_time=time.time(),
        )
        return [record]


@ray.remote
class FakeReader(Source):
    def __init__(
        self, num_keys, send_rate, timesteps=10000, cache_size=DEFAULT_STATE_CACHE_SIZE
    ):
        import pandas as pd

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
        import pandas as pd

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
