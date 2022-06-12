import functools
import heapq
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import total_ordering
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
    _operator = None

    def get(self, key): 
        assert self._operator is not None, f"Operator not set {self._operator}"
        return self._operator.get(key)

    def wake_waiter_if_needed(self):
        if self.waker is not None:
            self.waker.set()
            self.waker = None

    def new_waker(self):
        # assert self.waker is None
        self.waker = self.event_class()
        return self.waker

    @abstractmethod
    def push_event(self, record: Record):
        pass

    @abstractmethod
    def pop_event(self) -> Record:
        pass

    @abstractmethod
    def qsize(self) -> int:
        pass

    def __repr__(self):
        return self.__class__.__name__


# TODO(simon): figure out a way to attach this generically to all scheduler
global_lock = threading.Lock()


def synchronized(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        with global_lock:
            return f(*args, **kwargs)

    return wrapper


class FIFO(BaseScheduler):
    def __init__(self) -> None:
        self.queue: List[Record] = []
        self.waker: Optional[threading.Event] = None

    @synchronized
    def push_event(self, record: Record):
        self.wake_waiter_if_needed()
        self.queue.append(record)

    @synchronized
    def pop_event(self) -> Record:
        if len(self.queue) == 0:
            return Record.make_wait_event(self.new_waker())

        return self.queue.pop(0)

    def qsize(self) -> int:
        return len(self.queue)


class LIFO(BaseScheduler):
    def __init__(self) -> None:
        self.queue: List[Record] = []
        self.waker: Optional[threading.Event] = None

    @synchronized
    def push_event(self, record: Record):
        self.wake_waiter_if_needed()
        if record.is_stop_iteration():
            self.queue.insert(0, record)
        else:
            self.queue.append(record)

    @synchronized
    def pop_event(self) -> Record:
        if len(self.queue) == 0:
            return Record.make_wait_event(self.new_waker())
        return self.queue.pop(-1)

    def qsize(self) -> int:
        return len(self.queue)

@total_ordering
class KeyCount:
    def __init__(self, key, num_processed, record):
        self.key = key
        self.records = [record]
        self.num_processed = num_processed

    def process(self):
        self.num_processed += 1
        return self.records.pop()

    def add_record(self, record):
        self.records.append(record)

    def __eq__(self, other):
        return self.key == other.key and self.num_processed == other.num_processed

    def __lt__(self, other):
        return (
            int(len(self.records) == 0) * 1000000000 + self.num_processed
            < other.num_processed + int(len(other.records) == 0) * 1000000000
        )

    def __gt__(self, other):
        return (
            int(len(self.records) == 0) * 1000000000 + self.num_processed
            > other.num_processed + int(len(other.records) == 0) * 1000000000
        )

    def __repr__(self):
        return f"KeyCount(key : {self.key}, num_processed : {self.num_processed}, # of records : {len(self.records)}"


class LeastUpdate(BaseScheduler):
    def __init__(self) -> None:
        self.seen_keys = dict()
        self.queue: List[KeyCount] = []
        self.waker: Optional[threading.Event] = None
        self.num_unprocessed = 0

    def push_event(self, record: Record):
        if record.is_stop_iteration():
            kc = KeyCount("stop", 0, record)
            self.queue.insert(0, kc)
            return
        key = record.entry.key
        if key not in self.seen_keys:
            kc = KeyCount(key, 0, record)
            self.seen_keys[key] = kc
            heapq.heappush(self.queue, kc)
        else:
            kc = self.seen_keys[key]
            kc.add_record(record)

            heapq.heapify(self.queue)
            # print(self.queue[0])
            if len(self.queue[0].records) == 0:
                print(
                    self.num_unprocessed,
                    sum([len(i.records) for i in self.queue]),
                    [[len(i.records), i.key, i.num_processed] for i in self.queue],
                )
        self.wake_waiter_if_needed()

    def pop_event(self) -> Record:
        if not self.queue or len(self.queue[0].records) == 0:
            return Record.make_wait_event(self.new_waker())
        least_updated = heapq.heappop(self.queue)
        if len(least_updated.records) == 0:
            return Record.make_wait_event(self.new_waker())
        record = least_updated.process()
        # self.num_unprocessed -= 1
        heapq.heappush(self.queue, least_updated)
        return record


@dataclass
class DummyEntry:
    pass


class SourceScheduler(BaseScheduler):
    """A scheduler that always return dummy record."""

    def push_event(self, record: Record):
        pass

    def pop_event(self) -> Record:
        return Record(DummyEntry())

    def qsize(self) -> int:
        return 0


# TODO(simon): scheduler ideas
# MultilevelComposingScheduler
# CachingEventScheduler
