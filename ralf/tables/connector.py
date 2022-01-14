from abc import ABC, abstractmethod
from typing import List, Union

from ralf.state import Record, Schema


class Connector(ABC):
    @abstractmethod
    def add_table(self, schema: Schema):
        pass

    @abstractmethod
    def update(self, schema: Schema, record: Record):
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
    def count(self, schema: Schema) -> int:
        pass
