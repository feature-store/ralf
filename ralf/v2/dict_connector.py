from typing import Dict, List, Union

from ralf.v2.record import Record, Schema
from ralf.v2.connector import Connector


class DictConnector(Connector):
    def __init__(self):
        self.tables = dict()

    def add_table(self, schema: Schema):
        self.tables[schema.name] = dict()

    def get_records(self, schema: Schema) -> Dict:
        return self.tables[schema.get_name()]

    def update(self, schema: Schema, record: Record):
        records = self.get_records(schema)
        key = getattr(record.entry, schema.primary_key)
        records[key] = record

    def delete(self, schema: Schema, key: str):
        records = self.get_records(schema)
        if key in records:
            records.pop(key, None)

    def get_one(self, schema: Schema, key) -> Union[Record, None]:
        records = self.get_records(schema)
        if key in records:
            return records[key]
        return None

    def get_all(self, schema: Schema) -> List[Record]:
        records = self.get_records(schema)
        return list(records.values())

    def count(self, schema: Schema) -> int:
        records = self.get_records(schema)
        return len(records.items())
