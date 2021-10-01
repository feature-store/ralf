from abc import ABC, abstractmethod
from typing import List, Optional

from ralf.operator import DEFAULT_STATE_CACHE_SIZE, ActorPool, Operator
from ralf.state import Record, Schema, TableState


class LeftJoin(Operator, ABC):
    def __init__(
        self,
        schema: Schema,
        left_schema: Schema,
        right_schema: Schema,
        cache_size=DEFAULT_STATE_CACHE_SIZE,
        lazy: bool = False,
    ):
        super().__init__(schema, cache_size, lazy)
        self.left_table = TableState(schema=left_schema)
        self.right_table = TableState(schema=right_schema)

        # handles didn't match if left as ActorHandle, so extract id
        self.left_ids = []
        self.right_ids = []

    def set_parents(self, parents: List[ActorPool]):
        # override to set left/right ids
        self._parents = parents
        self.left_ids = [handle._actor_id for handle in self._parents[0].handles]
        self.right_ids = [handle._actor_id for handle in self._parents[1].handles]

    def on_record(self, record: Record) -> Optional[Record]:
        if record._source._actor_id in self.left_ids:
            key = getattr(record, self.left_table.schema.primary_key)
            right_record = self.right_table.records.get(key, None)
            if right_record is None:
                self.left_table.update(record)
            else:
                return self.join(record, right_record)
        elif record._source._actor_id in self.right_ids:
            self.right_table.update(record)
            key = getattr(record, self.right_table.schema.primary_key)
            left_record = self.left_table.records.get(key, None)
            if left_record is not None:
                self.left_table.delete(key)
                return self.join(left_record, record)
        else:
            raise ValueError("Actor id not found in left or right table")

    @abstractmethod
    def join(self, left_record: Record, right_record: Record) -> Record:
        pass

    def evict(self, key: str):
        super().evict(key)
        # TODO: evict left and right tables?
        raise NotImplementedError

    def send(self, record: Record):
        # TODO: manage size of left and right input tables?
        super().send(record)
