import json
import logging
import time
from typing import List, Optional, Type

import ray

from ralf.operator import DEFAULT_STATE_CACHE_SIZE, Operator
from ralf.state import Record, Schema

logger = logging.getLogger()


@ray.remote
class TumblingWindow(Operator):
    def __init__(
        self,
        size: int,
        primary_key: str,
        primary_key_type: Type,
        cache_size=DEFAULT_STATE_CACHE_SIZE,
        num_worker_threads: int = 1,
    ):
        # TODO: generate schema automatically from generics
        schema = Schema(
            primary_key,
            {
                primary_key: primary_key_type,
                "window": List[Record],
            },
        )
        super().__init__(schema, cache_size, num_worker_threads=num_worker_threads)
        self.size = size
        self.windows = {}

    def on_record(self, record) -> Optional[Record]:
        key = getattr(record, self._table.schema.primary_key)
        window = self.windows.get(key, None)
        if window is None:
            window = []
            self.windows[key] = window

        window.append(record)
        if len(window) >= self.size:
            # send update to children
            window_record = Record(
                **{self._table.schema.primary_key: key, "window": window}
            )

            self.windows[key] = []
            return window_record


@ray.remote
class SlidingWindow(Operator):
    def __init__(
        self,
        window_size: int,
        slide_size: int,
        primary_key: str,
        primary_key_type: Type,
        cache_size=DEFAULT_STATE_CACHE_SIZE,
        num_worker_threads: int = 1,
        per_key_slide_size_plan_file: Optional[str] = None,
    ):
        """
        Args:
            per_key_slide_size: optionally override the slidesize and make it key aware.
        """
        # TODO: generate schema automatically from generics
        schema = Schema(
            primary_key,
            {
                primary_key: primary_key_type,
                "window": List[Record],
                "send_time": float,
                "create_time": float,
                "complete_time": float,
                "timestamp": int,
            },
        )
        super().__init__(schema, cache_size, num_worker_threads=num_worker_threads)
        self.global_slide_size = slide_size
        self.window_size = window_size
        self.windows = {}
        self.max_create_time = {}
        self.max_timestamp = {}
        if per_key_slide_size_plan_file:
            with open(per_key_slide_size_plan_file) as f:
                self.per_key_slide_size = json.load(f)

    def on_record(self, record) -> Optional[Record]:
        try:
            key = getattr(record, self._table.schema.primary_key)
            window = self.windows.get(key, None)
            slide_size = (
                self.per_key_slide_size[key]
                if self.per_key_slide_size is not None
                else self.global_slide_size
            )
            if window is None:
                window = []

            window.append(record)
            self.max_create_time[key] = record.create_time
            self.max_timestamp[key] = record.timestamp
            if len(window) >= self.window_size:
                # send update to children
                window_record = Record(
                    **{
                        self._table.schema.primary_key: key,
                        "window": window,
                        "create_time": self.max_create_time[key],
                        "complete_time": time.time(),
                        "timestamp": self.max_timestamp[key],
                    }
                )

                self.windows[key] = window[slide_size:]
                return window_record
            else:
                self.windows[key] = window
        except Exception:
            logger.exception("Exception in sliding window")
