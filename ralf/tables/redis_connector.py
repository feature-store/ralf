import json
from typing import List, Union

import redis

from ralf.state import Record, Schema
from ralf.tables.connector import Connector


class RedisConnector(Connector):
    # Not sure how historical would work with Redis so ignoring it for now

    def __init__(self, conn: redis.client.Redis):
        self.conn = conn

    def add_table(self, schema: Schema, historical: bool):
        pass

    def update(self, schema: Schema, historical: bool, record: Record):
        key = getattr(record, schema.primary_key)
        jsonString = json.dumps(vars(record))
        self.conn.hset(schema.get_name(), key, jsonString)

    def delete(self, schema: Schema, key: str):
        self.conn.hdel(schema.get_name(), key)

    def get_one(self, schema: Schema, key) -> Union[Record, None]:
        val = self.conn.hget(schema.get_name(), key)
        if val:
            record = val.decode("utf-8")
            return Record(**json.loads(record))
        return None

    def get_all(self, schema: Schema) -> List[Record]:
        values = self.conn.hvals(schema.get_name())
        records = [Record(**json.loads(val.decode("utf-8"))) for val in values]
        return sorted(records, key=lambda r: r.processing_time)

    def get_num_records(self, schema: Schema) -> int:
        num_records = self.conn.hlen(schema.get_name())
        return num_records
