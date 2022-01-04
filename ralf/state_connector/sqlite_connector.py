from ralf.state_connector.connector import Connector
from ralf.state import Record, Schema
from typing import List, Tuple
import sqlite3

class SQLiteConnector(Connector):

    def __init__(self, table_name: str, sql_conn: sqlite3.Connection, schema: Schema, historical: bool = False):
        self.table_name = table_name
        self.sql_conn = sql_conn
        self.schema = schema
        self.historical = historical
        if not historical:
            self.sql_conn.execute('DROP TABLE IF EXISTS {table_name}')
        if not self.schema.sql_check():
            raise TypeError("The schema is invalid for SQL") 
        table_format = self.schema.sql_format()
        self.sql_conn.execute('''CREATE TABLE IF NOT EXISTS {table_name} 
                                    ({table_format})''')
        self.sql_conn.commit()

    def update(self, record: Record):
        # modify for historical
        curr = self.sql_conn.cursor()
        update_format = record.sql_update_format()
        values = record.sql_values(
        curr.execute('''IF EXISTS(SELECT * FROM {self.table_name} WHERE {self.primary_key} = {record.key}) 
                            UPDATE {self.table_name} SET {update_format} WHERE {self.primary_key} = {record.key}
                        ELSE
                            INSERT INTO {self.table_name} VALUES ({values})''')
        self.sql_conn.commit()

    def delete(self, key: str):
        # modify for historical
        curr = self.sql_conn.cursor()
        curr.execute("DELETE FROM {self.table_name} WHERE {self.primary_key} = {key}")
        self.sql_conn.commit()

    def row_to_record(self, row: Tuple):
        schema_columns_repeated = [self.schema.primary_key] + self.schema.columns.keys())
        schema_columns = list(dict.fromkeys(schema_columns_repeated)) # without the duplicate primary key
        entries = dict()
        for i in range(len(schema_columns)):
            entries[schema_columns[i]] = row[i]
        processing_time = row[-1]
        return Record(entries, processing_time)

    def point_query(self, key) -> Record:
        # modify for historical
        curr = self.sql_conn.cursor()
        row = curr.execute("SELECT * FROM {self.table_name} WHERE {self.primary_key} = {key}").fetch_one()
        record = self.row_to_record(i, schema)
        return record


    def bulk_query(self) -> List[Record]:
        # modify for historical
        curr = self.sql_conn.cursor()
        rows = curr.execute("SELECT * FROM {self.table_name}").fetch_all()
        records = [self.row_to_record(i) for i in rows]
        return records

    
    