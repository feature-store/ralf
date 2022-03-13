import pickle
from typing import List, Union

import redis

from ralf.record import Record, Schema
from ralf.state.connector import Connector


class RedisConnector(Connector):
    def __init__(self, conn: redis.client.Redis):
        self.conn = conn

    def add_table(self, schema: Schema):
        pass

    def update(self, schema: Schema, record: Record):
        key = getattr(record, schema.primary_key)
        pickled_record = pickle.dumps(record)
        self.conn.hset(schema.get_name(), key, pickled_record)

    def delete(self, schema: Schema, key: str):
        self.conn.hdel(schema.get_name(), key)

    def get_one(self, schema: Schema, key) -> Union[Record, None]:
        val = self.conn.hget(schema.get_name(), key)
        if val:
            return pickle.loads(val)
        return None

    def get_all(self, schema: Schema) -> List[Record]:
        values = self.conn.hvals(schema.get_name())
        records = [pickle.loads(val) for val in values]
        return sorted(records, key=lambda r: r.processing_time)

    def count(self, schema: Schema) -> int:
        num_records = self.conn.hlen(schema.get_name())
        return num_records
