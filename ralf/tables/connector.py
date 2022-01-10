from abc import ABC, abstractmethod
from ralf.state import Record
from typing import Any, Dict, List, Type, Union
from ralf.state import Schema

class Connector(ABC):

    @abstractmethod
    def add_table(self, schema: Schema, historical: bool):
        pass

    @abstractmethod
    def update(self, schema: Schema, historical: bool, record: Record):
        pass

    @abstractmethod
    def delete(self, schema: Schema, key: str):
        pass

    @abstractmethod
    def get_one(self, schema: Schema, key: str) -> Union[Record, None]:
        pass

    @abstractmethod
    def get_all(self, schema: Schema) -> List[Record]:
        pass
    
    @abstractmethod
    def get_num_records(self, schema: Schema) -> int:
        pass
    