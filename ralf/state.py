import time
from typing import Any, Dict, List, Type


class Record:
    def __init__(self, **entries: Dict[str, Any]):
        self.entries: Dict[str, Any] = entries
        self._source = None
        self.processing_time = time.time()

        for k, v in entries.items():
            setattr(self, k, v)

    # def __str__(self):
    #     return str(self.entries)

    def __repr__(self):
        return f"Record(key={self.key},processing_time={self.processing_time:.2f})"

    def __eq__(self, other: "Record") -> bool:
        return self.entries == other.entries and self._source == other._source

    # TODO: Uhh this makes ray not work
    # def __dict__(self):
    #    return self.entries


class Schema:
    def __init__(self, primary_key: str, columns: Dict[str, Type]):
        self.primary_key = primary_key
        self.columns = columns

    def validate_record(self, record: Record):
        # TODO: add type checking.
        schema_columns = set(self.columns.keys()).union(set([self.primary_key]))
        record_columns = set(record.entries.keys())
        assert (
            schema_columns == record_columns
        ), f"schema columns are {schema_columns} but record has {record_columns}"


# Maintains table values
# TODO: This should eventually be a wrapper around a DB connection
class TableState:
    def __init__(self, schema: Schema):
        self.schema = schema
        self.records = {}

        self.num_updates: int = 0
        self.num_deletes: int = 0
        self.num_records: int = 0

    def debug_state(self):
        return {
            "num_updates": self.num_updates,
            "num_deletes": self.num_deletes,
            "num_records": self.num_records,
        }

    def update(self, record: Record):
        key = getattr(record, self.schema.primary_key)
        self.records[key] = record

        self.num_updates += 1
        self.num_records = len(self.records)

    def delete(self, key: str):
        self.records.pop(key, None)
        self.num_deletes += 1

    def get_schema(self) -> Schema:
        return self.schema

    def point_query(self, key) -> Record:
        if key not in self.records:
            raise KeyError(f"Key {key} not found.")
        return self.records[key]

    def bulk_query(self) -> List[Record]:
        return list(self.records.values())
