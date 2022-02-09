import time
from dataclasses import make_dataclass

import numpy as np
import pytest

from ralf.v2 import LIFO, BaseTransform, RalfApplication, RalfConfig, Record


@pytest.mark.parametrize("deploy_mode", ["local", "ray"])
def test_simple_lifo(deploy_mode):
    app = RalfApplication(RalfConfig(deploy_mode=deploy_mode))

    IntValue = make_dataclass("IntValue", ["value"])

    class CounterSource(BaseTransform):
        def __init__(self, up_to: int) -> None:
            self.count = 0
            self.up_to = up_to

        def on_event(self, _: Record) -> Record[IntValue]:
            self.count += 1
            if self.count >= self.up_to:
                raise StopIteration()
            return Record(IntValue(value=self.count))

    class Sum(BaseTransform):
        def __init__(self) -> None:
            self.state = 0
            self.history = []

        def on_event(self, record: Record[IntValue]):
            self.history.append(record.entries.value)
            self.state += record.entries.value
            time.sleep(0.1)
            return None

    sink = app.source(CounterSource(10)).transform(Sum(), LIFO())
    app.deploy()
    app.wait()

    operator = app.manager.operators[sink]
    transform_object = operator.dump_transform_state()[0]
    assert transform_object.state == 45
    event_history = transform_object.history
    assert np.argmax(event_history) != (
        len(event_history) - 1
    ), "Top increment should be in the last place for LIFO"
