import json
import time
from string import ascii_lowercase
from typing import Any, Dict, Type


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

    def sql_update_format(self) -> str:
        query = []
        for k, v in entries.items():
            query.append("{k} = {v}")
        query.append("processing_time = {self.processing_time}")
        return ", ".join(query)

    def sql_values(self) -> str:
        query = []
        for _, v in entries.items():
            query.append(str(v))
        query.append(str(self.processing_time))
        return ", ".join(query)


class Schema:
    def __init__(self, primary_key: str, columns: Dict[str, Type]):
        self.primary_key = primary_key
        self.columns = columns
        self.name = self.compute_name()

    def validate_record(self, record: Record):
        # TODO: add type checking.
        schema_columns = set(self.columns.keys()).union(set([self.primary_key]))
        record_columns = set(record.entries.keys())
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
