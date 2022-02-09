import json
import time
from string import ascii_lowercase
from typing import Any, Dict, Type


class Record:
    def __init__(self, **entry: Dict[str, Any]):
        self.entry: Dict[str, Any] = entry
        self._source = None
        self.processing_time = time.time()

        for k, v in entry.items():
            setattr(self, k, v)

    # def __str__(self):
    #     return str(self.entry)

    def __repr__(self):
        return f"Record({self.entry},processing_time={self.processing_time:.2f})"

    def __eq__(self, other: "Record") -> bool:
        return self.entry == other.entry and self._source == other._source

    # TODO: Uhh this makes ray not work
    # def __dict__(self):
    #    return self.entry


class Schema:
    def __init__(self, primary_key: str, columns: Dict[str, Type]):
        self.primary_key = primary_key
        self.columns = columns
        self.name = self.compute_name()

    def validate_record(self, record: Record):
        # TODO: add type checking.
        schema_columns = set(self.columns.keys()).union(set([self.primary_key]))
        record_columns = set(record.entry.keys())
        assert (
            schema_columns == record_columns
        ), f"schema columns are {schema_columns} but record has {record_columns}"

    def compute_name(self) -> str:
        dump = json.dumps(list(self.columns.keys()), sort_keys=True)
        hash_val = str(abs(hash(dump)))
        name = ""
        for c in hash_val:
            name += ascii_lowercase[int(c)]
        return name

    def get_name(self) -> str:
        return self.name

    def __hash__(self) -> int:
        return hash(self.name)
