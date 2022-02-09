import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Type

from typing_extensions import Protocol, runtime_checkable

from ralf.v2.record import Record


@runtime_checkable
class WakerProtocol(Protocol):
    def set(self):
        ...

    def wait(self, timeout=None):
        ...


class BaseScheduler(ABC):
    """Base class for scheduling event on to transform operator.

    Subclass should implement `push_event` for accepting new record
    and `pop_event` for the transform object asking for record to be
    worked on.
    """

    event_class: Type[WakerProtocol] = threading.Event

    def wake_waiter_if_needed(self):
        if self.waker is not None:
            self.waker.set()
            self.waker = None

    def new_waker(self):
        assert self.waker is None
        self.waker = self.event_class()
        return self.waker

    @abstractmethod
    def push_event(self, record: Record):
        pass

    @abstractmethod
    def pop_event(self) -> Record:
        pass


class FIFO(BaseScheduler):
    def __init__(self) -> None:
        self.queue: List[Record] = []
        self.waker: Optional[threading.Event] = None

    def push_event(self, record: Record):
        self.wake_waiter_if_needed()
        self.queue.append(record)

    def pop_event(self) -> Record:
        if len(self.queue) == 0:
            return Record.make_wait_event(self.new_waker())
        return self.queue.pop(0)


class LIFO(BaseScheduler):
    def __init__(self) -> None:
        self.queue: List[Record] = []
        self.waker: Optional[threading.Event] = None

    def push_event(self, record: Record):
        self.wake_waiter_if_needed()
        if record.is_stop_iteration():
            self.queue.insert(0, record)
        else:
            self.queue.append(record)

    def pop_event(self) -> Record:
        if len(self.queue) == 0:
            return Record.make_wait_event(self.new_waker())
        return self.queue.pop(-1)


@dataclass
class DummyEntry:
    pass


class SourceScheduler(BaseScheduler):
    """A scheduler that always return dummy record."""

    def push_event(self, record: Record):
        pass

    def pop_event(self) -> Record:
        return Record(DummyEntry())


# TODO(simon): scheduler ideas
# MultilevelComposingScheduler
# CachingEventScheduler
