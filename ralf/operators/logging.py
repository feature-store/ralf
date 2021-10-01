from abc import ABC, abstractmethod
from typing import Optional, Type

import ray

from ralf.operator import Operator, DEFAULT_STATE_CACHE_SIZE
from ralf.state import TableState, Record, Schema


@ray.remote
class Print(Operator):
    def __init__(
        self,
        primary_key: str,
        primary_key_type: Type,
        cache_size=DEFAULT_STATE_CACHE_SIZE,
    ):
        # TODO: generate schema automatically from generics
        schema = Schema(primary_key, {primary_key: primary_key_type})
        super().__init__(schema, cache_size)

    def on_record(self, record) -> Optional[Record]:
        print(record)
