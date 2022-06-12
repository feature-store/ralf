from collections import defaultdict
from tokenize import String
from typing import List
import time

from ralf.v2.record import Record, Schema
from ralf.v2.connector import Connector


# Maintains table values
# TODO: This should eventually be a wrapper around a DB connection
class TableState:
    def __init__(self, schema: Schema, connector: Connector, dataclass):
        self.schema = schema
        self.connector = connector
        self.connector.add_table(schema)
        self.dataclass = dataclass

        self.times = defaultdict(float)
        self.counts = defaultdict(int)

    def debug_state(self):
        self.num_records = self.connector.count(self.schema)
        return {
            "num_updates": self.counts["update"],
            "num_deletes": self.counts["delete"],
            "num_point_query": self.counts["point_query"],
        }

    def update(self, record: Record):
        self.schema.validate_record(record)        
        t1 = time.time()
        self.connector.update(self.schema, record)
        t2 = time.time()

        self.recordQuery("update", t2-t1)

    def delete(self, key: str):
        t1 = time.time()
        self.connector.delete(self.schema, key)
        t2 = time.time()

        self.recordQuery("delete", t2-t1)

    def point_query(self, key) -> Record:
        t1 = time.time()
        val = self.connector.get_one(self.schema, key, self.dataclass)
        t2 = time.time()

        if not val:
            raise KeyError(f"Key {key} not found.")
        self.recordQuery("point_query", t2-t1)
        return val

    def bulk_query(self) -> List[Record]:
        return self.connector.get_all(self.schema, self.dataclass)

    def get_schema(self) -> Schema:
        return self.schema

    def recordQuery(self, queryType: String, timeTaken):
        self.times[queryType] += timeTaken
        self.counts[queryType] += 1        
