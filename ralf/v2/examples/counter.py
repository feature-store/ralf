import time
from collections import defaultdict
from dataclasses import dataclass
from typing import List

from ralf.v2 import BaseTransform, RalfApplication, RalfConfig, Record
from ralf.v2.operator import OperatorConfig, RayOperatorConfig


@dataclass
class SourceValue:
    key: str
    value: int
    timestamp: float


@dataclass
class SumValue:
    key: str
    value: int


class FakeSource(BaseTransform):
    def __init__(self, total: int) -> None:
        self.count = 0
        self.total = total
        self.num_keys = 10

    def on_event(self, _: Record) -> List[Record[SourceValue]]:

        if self.count >= self.total:
            print("completed iteration")
            raise StopIteration()

        self.count += 1
        key = str(self.count % self.num_keys)

        # sleep to slow down send rate
        time.sleep(1)

        # return list of records (wrapped around dataclass)
        return [
            Record(
                entry=SourceValue(
                    key=key,
                    value=self.count,
                    timestamp=time.time(),
                ),
                shard_key=key,  # key to shard/query
            )
        ]


class Sum(BaseTransform):
    def __init__(self):
        self.total = defaultdict(lambda: 0)

    def on_event(self, record: Record) -> Record[SumValue]:
        self.total[record.entry.key] += record.entry.value
        print(f"Record {record.entry.key}, value {self.total[record.entry.key]}")
        return Record(
            entry=SumValue(key=record.entry.key, value=self.total[record.entry.key])
        )


if __name__ == "__main__":

    deploy_mode = "ray"
    # deploy_mode = "local"
    app = RalfApplication(RalfConfig(deploy_mode=deploy_mode))

    source_ff = app.source(
        FakeSource(10000),
        operator_config=OperatorConfig(
            ray_config=RayOperatorConfig(num_replicas=1),
        ),
    )

    sum_ff = source_ff.transform(
        Sum(),
        operator_config=OperatorConfig(
            ray_config=RayOperatorConfig(num_replicas=1),
        ),
    )

    app.deploy()
    app.wait()
