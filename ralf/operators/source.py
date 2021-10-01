from abc import ABC, abstractmethod
import asyncio
import traceback
from typing import List

from ray.actor import ActorHandle

from ralf.operator import Operator, DEFAULT_STATE_CACHE_SIZE
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
