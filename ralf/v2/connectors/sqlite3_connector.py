import pickle
import sqlite3
from typing import List, Union

from ralf.v2.record import Record, Schema
from ralf.v2.connectors.connector import Connector

sql_types = {int: "INTEGER", str: "TEXT", float: "REAL"}

def schema_sql_format(schema: Schema) -> str:
    query = []
    primary_type = sql_types[schema.columns[schema.primary_key]]
    query.append(f"{schema.primary_key} {primary_type}")
    query.append("record TEXT")
    return ", ".join(query)


class SQLConnector(Connector):
    def __init__(self, dbname: str):
        self.dbname = dbname

    def add_table(self, schema: Schema):
        conn = sqlite3.connect(self.dbname)
        table_name = schema.get_name()
        curr = conn.cursor()
        if schema.columns[schema.primary_key] not in sql_types:
            raise TypeError("The schema is invalid for SQL")
        curr.execute(f"DROP TABLE IF EXISTS {table_name}")
        table_format = schema_sql_format(schema)
        curr.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({table_format})")
        conn.commit()

    def update(self, schema: Schema, record: Record):
        conn = sqlite3.connect(self.dbname)
        curr = conn.cursor()
        table_name = schema.get_name()
        pickled_record = pickle.dumps(record)
        delete_statement = f"DELETE FROM {table_name} WHERE key = {record.key}"
        insert_statement = (
            f"INSERT INTO {table_name} (key, record) VALUES ({record.key}, ?)"
        )
        curr.execute(delete_statement)
        curr.execute(insert_statement, (pickled_record,))
        conn.commit()

    def delete(self, schema: Schema, key: str):
        conn = sqlite3.connect(self.dbname)
        curr = conn.cursor()
        table_name = schema.get_name()
        curr.execute(f"DELETE FROM {table_name} WHERE {schema.primary_key} = {key}")
        conn.commit()

    def get_one(self, schema: Schema, key: str) -> Union[Record, None]:
        conn = sqlite3.connect(self.dbname)
        curr = conn.cursor()
        table_name = schema.get_name()
        select_statement = f"SELECT record FROM {table_name} WHERE {schema.primary_key} = {key} ORDER BY rowid DESC"
        row = curr.execute(select_statement).fetchone()
        record = None
        if row:
            record = pickle.loads(row[0])
        return record

    def get_all(self, schema: Schema) -> List[Record]:
        conn = sqlite3.connect(self.dbname)
        curr = conn.cursor()
        table_name = schema.get_name()
        rows = curr.execute(f"SELECT record FROM {table_name}").fetchall()
        records = [pickle.loads(i[0]) for i in rows]
        return records

    def count(self, schema: Schema) -> int:
        conn = sqlite3.connect(self.dbname)
        curr = conn.cursor()
        table_name = schema.get_name()
        count = curr.execute(f"SELECT COUNT(rowid) FROM {table_name}").fetchone()[0]
        return count