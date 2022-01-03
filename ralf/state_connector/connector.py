from abc import ABC, abstractmethod
from ralf.state import Record
from typing import Any, Dict, List, Type

class Connector(ABC):

    @abstractmethod
    def on_update(self, record: Record):
        pass

    @abstractmethod
    def delete(self, key: str):
        pass

    @abstractmethod
    def point_query(self, key) -> Record:
        pass

    @abstractmethod
    def bulk_query(self) -> List[Record]:
        pass

    def get_schema(self) -> Schema:
        return self.schema
    
    