import pickle
from typing import List, Union

import redis

from ralf.v2.record import Record, Schema
from ralf.v2.connector import Connector

class RedisConnector(Connector):
    def __init__(self, host: str, port: str, password: str):
        self.host = host
        self.port = port
        self.password = password

    def create_connection(self):
        return RedisConnector(self.host, self.port, self.password)

    def get_conn(self):
        return redis.Redis(host=self.host, port=self.port, password=self.password)

    def add_table(self, schema: Schema):
        pass

    def update(self, schema: Schema, record: Record):
        key = getattr(record.entry, schema.primary_key)
        pickled_record = Record.serialize(record)
        self.conn.hset(schema.get_name(), key, pickled_record)

    def delete(self, schema: Schema, key: str):
        self.conn.hdel(schema.get_name(), key)

    def get_one(self, schema: Schema, key, dataclass) -> Union[Record, None]:
        val = self.conn.hget(schema.get_name(), key)
        if val:
            return Record.deserialize(val, dataclass)
        return None

    def get_all(self, schema: Schema, dataclass) -> List[Record]:
        values = self.conn.hvals(schema.get_name())
        records = [Record.deserialize(val, dataclass) for val in values]
        return sorted(records, key=lambda r: r.entry.timestamp)

    def count(self, schema: Schema) -> int:
        num_records = self.conn.hlen(schema.get_name())
        return num_records

    def prepare(self):
        self.conn = redis.Redis(host=self.host, port=self.port, password=self.password)