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
