from typing import List

from ralf.v2.record import Record, Schema
from ralf.v2.connector import Connector


# Maintains table values
# TODO: This should eventually be a wrapper around a DB connection
class TableState:
    def __init__(self, schema: Schema, connector: Connector):
        self.schema = schema
        self.connector = connector
        self.connector.add_table(schema)

        self.num_updates: int = 0
        self.num_deletes: int = 0
        self.num_records: int = 0

    def debug_state(self):
        self.num_records = self.connector.count(self.schema)
        return {
            "num_updates": self.num_updates,
            "num_deletes": self.num_deletes,
            "num_records": self.num_records,
        }

    def update(self, record: Record):
        self.schema.validate_record(record)
        self.connector.update(self.schema, record)
        self.num_updates += 1

    def delete(self, key: str):
        self.connector.delete(self.schema, key)
        self.num_deletes += 1

    def point_query(self, key) -> Record:
        val = self.connector.get_one(self.schema, key)
        if not val:
            raise KeyError(f"Key {key} not found.")
        return val

    def bulk_query(self) -> List[Record]:
        return self.connector.get_all(self.schema)

    def get_schema(self) -> Schema:
        return self.schema
