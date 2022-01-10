from ralf.tables.connector import Connector
import redis
from ralf.state import Record, Schema
import json
from typing import List

class RedisConnector(Connector):

    def __init__(self, name: str, redis_conn: redis.client.Redis, schema: Schema, historical: bool = False):
        self.name = name
        self.historical = historical
        self.redis_conn = redis_conn
        self.schema = schema

    def update(self, record: Record):
        if not self.historical:
            key = getattr(record, self.schema.primary_key)
            val = json.dumps(record.entries)
            self.redis_conn.set(key, val)

    def delete(self, key: str):
        return self.redis_conn.delete(key)

    @abstractmethod
    def point_query(self, key) -> Record:
        val = self.redis_conn.get(key)
        if val:
            r = Record(val) 
            return r
        else:
            raise KeyError(f"Key {key} not found.")
    
    @abstractmethod
    def bulk_query(self) -> List[Record]:
        if val:
            r = Record(val) 
            return r
        else:
            raise KeyError(f"Key {key} not found.")
