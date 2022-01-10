from typing import Optional, Type

import ray

from ralf.operator import DEFAULT_STATE_CACHE_SIZE, Operator
from ralf.state import Record, Schema
from ralf.tables import Connector

@ray.remote
class Print(Operator):
    def __init__(
        self,
        primary_key: str,
        primary_key_type: Type,
        connector: Connector,
        historical: bool = False,
        cache_size=DEFAULT_STATE_CACHE_SIZE,
    ):
        # TODO: generate schema automatically from generics
        schema = Schema(primary_key, {primary_key: primary_key_type})
        super().__init__(schema, connector, cache_size, historical)

    def on_record(self, record) -> Optional[Record]:
        print(record)
