from abc import ABC, abstractmethod
from typing import List, Union

from ralf.record import Record, Schema


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
    def get_one(self, schema: Schema, key: str, dataclass) -> Union[Record, None]:
        pass

    @abstractmethod
    def get_all(self, schema: Schema, dataclass) -> List[Record]:
        pass

    @abstractmethod
    def count(self, schema: Schema) -> int:
        pass

    @abstractmethod
    def prepare(self) -> None:
        pass
