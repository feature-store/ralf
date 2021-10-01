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
        total_run_time: Optional[float] = None,
        data_file: Optional[str] = None,
    ):
        self.env = env
        self.records_per_sec_per_key = records_per_sec_per_key
        self.num_keys = num_keys
        self.next_queue = next_queue
        self.total_run_time = total_run_time
        self.data: Optional[List] = None

        if data_file is not None:
            print("Reading", data_file)
            self.data = []
            df = pd.read_csv(data_file)
            for index, row in df.iterrows():
                self.data.append(row.to_dict())

        self.env.process(self.run())

    def run(self):
        record_id = 0
        while True:
            if self.total_run_time and self.env.now > self.total_run_time:
                break

            for key in range(self.num_keys):
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
                            value=self.data[record_id]["value"],
                        )
                    )
            record_id += 1
            yield self.env.timeout(1 / (self.records_per_sec_per_key))
