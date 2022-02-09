import enum
import threading
from dataclasses import dataclass, is_dataclass
from typing import Generic, TypeVar, Union


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
    entries: Union[T, StopIteration, threading.Event]

    # signify a tagged union
    type_: RecordType = RecordType.DATA

    def __post_init__(self):
        assert self.type_ in RecordType
        if self.type_ == RecordType.DATA:
            assert self.is_data()
        elif self.type_ == RecordType.STOP_ITERATION:
            assert self.is_stop_iteration()
        elif self.type_ == RecordType.WAIT_EVENT:
            assert self.is_wait_event()
        else:
            raise ValueError("Unknown type.")

    @staticmethod
    def make_stop_iteration():
        return Record(StopIteration(), type_=RecordType.STOP_ITERATION)

    @staticmethod
    def make_wait_event(event: threading.Event):
        return Record(event, type_=RecordType.WAIT_EVENT)

    def is_data(self) -> bool:
        return is_dataclass(self.entries)

    def is_stop_iteration(self) -> bool:
        return isinstance(self.entries, StopIteration)

    def is_wait_event(self) -> bool:
        from ralf.v2.scheduler import WakerProtocol

        return isinstance(self.entries, WakerProtocol)

    def wait(self):
        assert self.is_wait_event()
        self.entries.wait()
