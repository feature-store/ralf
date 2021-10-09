import json
import os
from collections import defaultdict
from typing import List, Optional

import pandas as pd
import simpy

from ralf.state import Record


class Source:
    def __init__(
        self,
        env: simpy.Environment,
        records_per_sec_per_key: int,
        num_keys: int,
        next_queue: simpy.Store,
        keys: List[int],
        total_run_time: Optional[float] = None,
        data_dir: Optional[str] = None,
    ):
        self.env = env
        self.records_per_sec_per_key = records_per_sec_per_key
        self.num_keys = num_keys
        self.next_queue = next_queue
        self.total_run_time = total_run_time
        self.data: Optional[List] = None

        self.keys = list(keys)

        if data_dir is not None:
            self.data = {}
            for key in keys:
                self.data[key] = []
                data_file = os.path.join(data_dir, f"{key}.csv")
                df = pd.read_csv(data_file)
                for index, row in df.iterrows():
                    self.data[key].append(row.to_dict())
                print("Read", len(self.data[key]), data_file)

        for key in self.keys:
            assert len(self.data[key]) == len(self.data[self.keys[0]])
        self.ts_len = len(self.data[self.keys[0]])

        self.env.process(self.run())

    def run(self):
        record_id = 0
        while True:
            if record_id >= self.ts_len or self.env.now > self.total_run_time:
                break

            for key in self.keys:
                # TODO: add sleep here instead?
                if self.data is None:
                    yield self.next_queue.put(
                        Record(key=key, seq_id=record_id, processing_time=self.env.now)
                    )
                else:
                    yield self.next_queue.put(
                        Record(
                            key=key,
                            seq_id=record_id,
                            processing_time=self.env.now,
                            value=self.data[key][record_id]["value"],
                        )
                    )
            record_id += 1
            yield self.env.timeout(1 / (self.records_per_sec_per_key))


class JSONSource:
    def __init__(
        self,
        env: simpy.Environment,
        records_per_sec_per_key: int,
        num_keys: int,
        next_queue: simpy.Store,
        total_run_time: Optional[float] = None,
        data_file: Optional[str] = None,
    ):
        self.env = env
        self.records_per_sec_per_key = records_per_sec_per_key
        self.num_keys = num_keys
        self.next_queue = next_queue
        self.total_run_time = total_run_time
        self.data = json.load(open(data_file))
        self.index = 0
        self.optimal_plan = defaultdict(list)

        self.env.process(self.run())

    def run(self):
        record_id = 0
        while True:
            if (
                self.index >= len(self.data)
                or self.total_run_time
                and self.env.now > self.total_run_time
            ):
                print(
                    f"Complete source ts: {self.index}/{len(self.data)}, env time: {self.env.now}/{self.total_run_time}"
                )
                open("optimal_plan.json", "w").write(json.dumps(self.optimal_plan))
                break

            # send for each key at timestep
            for key in self.data[self.index].keys():
                edit = self.data[self.index][key]  # list of diff.json files
                # print(f"Send {self.index}, {self.env.now}: {edit}")
                self.optimal_plan[self.env.now] += [(e, key) for e in edit]
                yield self.next_queue.put(
                    Record(
                        key=key,  # doc_id
                        value=edit,
                        seq_id=record_id,
                        processing_time=self.env.now,
                    )
                )

            record_id += 1
            self.index += 1
            yield self.env.timeout(1 / (self.records_per_sec_per_key))
