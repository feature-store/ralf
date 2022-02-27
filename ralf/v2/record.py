import enum
import threading
from dataclasses import dataclass, is_dataclass
from time import time_ns
from typing import Generic, Optional, TypeVar, Union


class RecordType(enum.Enum):
    # User defined data
    DATA = enum.auto()
    # Signal end of the stream
    STOP_ITERATION = enum.auto()
    # Waiting token for next avaiable event
    WAIT_EVENT = enum.auto()


T = TypeVar("T")


@dataclass
class Record(Generic[T]):
    """Wrapper class for data "row" in transit within ralf."""

    # user provided data type
    entry: Union[T, StopIteration, threading.Event]

    # shard key
    shard_key: str = ""

    # signify a tagged union
    type_: RecordType = RecordType.DATA

    # unique id
    id_: Optional[int] = None

    def __post_init__(self):
        if self.id_ is None:
            self.id_ = time_ns()

        assert self.type_ in RecordType
        if self.type_ == RecordType.DATA:
            assert self.is_data()
        elif self.type_ == RecordType.STOP_ITERATION:
            assert self.is_stop_iteration()
        elif self.type_ == RecordType.WAIT_EVENT:
            assert self.is_wait_event()
        else:
            raise ValueError("Unknown type.")

        assert isinstance(self.shard_key, str)

    @staticmethod
    def make_stop_iteration():
        return Record(StopIteration(), type_=RecordType.STOP_ITERATION)

    @staticmethod
    def make_wait_event(event: threading.Event):
        return Record(event, type_=RecordType.WAIT_EVENT)

    def is_data(self) -> bool:
        return is_dataclass(self.entry)

    def is_stop_iteration(self) -> bool:
        return isinstance(self.entry, StopIteration)

    def is_wait_event(self) -> bool:
        from ralf.v2.scheduler import WakerProtocol

        return isinstance(self.entry, WakerProtocol)

    def wait(self):
        assert self.is_wait_event()
        self.entry.wait()
