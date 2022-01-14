import pickle
import sqlite3
from typing import List, Union

from ralf.state import Record, Schema
from ralf.tables.connector import Connector

sql_types = {int: "INTEGER", str: "TEXT", float: "REAL"}


def schema_sql_format(schema: Schema) -> str:
    query = []
    table_id = "id INT IDENTITY PRIMARY KEY"
    query.append(table_id)
    primary_type = sql_types[schema.columns[schema.primary_key]]
    query.append(f"{schema.primary_key} {primary_type}")
    query.append("record TEXT")
    return ", ".join(query)


class SQLiteConnector(Connector):
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def add_table(self, schema: Schema):
        table_name = schema.get_name()
        curr = self.conn.cursor()
        if schema.columns[schema.primary_key] not in sql_types:
            raise TypeError("The schema is invalid for SQL")
        curr.execute(f"DROP TABLE IF EXISTS {table_name}")
        table_format = schema_sql_format(schema)
        curr.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({table_format})")
        self.conn.commit()

    def update(self, schema: Schema, record: Record):
        curr = self.conn.cursor()
        table_name = schema.get_name()
        pickled_record = pickle.dumps(record)
        delete_statement = f"DELETE FROM {table_name} WHERE key = {record.key}"
        insert_statement = (
            f"INSERT INTO {table_name} (key, record) VALUES ({record.key}, ?)"
        )
        curr.execute(delete_statement)
        curr.execute(insert_statement, (pickled_record,))
        self.conn.commit()

    def delete(self, schema: Schema, key: str):
        curr = self.conn.cursor()
        table_name = schema.get_name()
        curr.execute(f"DELETE FROM {table_name} WHERE {schema.primary_key} = {key}")
        self.conn.commit()

    def get_one(self, schema: Schema, key: str) -> Union[Record, None]:
        curr = self.conn.cursor()
        table_name = schema.get_name()
        select_statement = f"SELECT record FROM {table_name} WHERE {schema.primary_key} = {key} ORDER BY id DESC"
        row = curr.execute(select_statement).fetchone()
        record = None
        if row:
            record = pickle.loads(row[0])
        return record

    def get_all(self, schema: Schema) -> List[Record]:
        curr = self.conn.cursor()
        table_name = schema.get_name()
        rows = curr.execute(f"SELECT record FROM {table_name}").fetchall()
        records = [pickle.loads(i[0]) for i in rows]
        return records

    def count(self, schema: Schema) -> int:
        curr = self.conn.cursor()
        schema.get_name()
        count = curr.execute("SELECT COUNT(id) FROM {table_name}").fetch_one()
        return count
