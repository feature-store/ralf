from abc import ABC, abstractmethod
from typing import List

from ray.actor import ActorHandle

from ralf.operator import Operator, DEFAULT_STATE_CACHE_SIZE
from ralf.state import TableState, Record, Schema


class Sink(Operator):
    def __init__(self, schema: Schema, cache_size=DEFAULT_STATE_CACHE_SIZE, **kwargs):
        super().__init__(schema, cache_size, lazy=False, **kwargs)

    def on_record(self, record: Record):
        # Optionally write to sink DB
        return record
