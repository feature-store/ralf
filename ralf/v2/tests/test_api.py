import time
from dataclasses import make_dataclass

import numpy as np
import pytest
import simpy

from ralf.v2 import LIFO, BaseTransform, RalfApplication, RalfConfig, Record
from ralf.v2.operator import OperatorConfig, SimpyOperatorConfig

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
        self.history.append(record.entry.value)
        self.state += record.entry.value
        time.sleep(0.1)
        return None


@pytest.mark.parametrize("deploy_mode", ["local", "ray"])
def test_simple_lifo(deploy_mode):
    app = RalfApplication(RalfConfig(deploy_mode=deploy_mode))

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


def test_simpy_lifo():
    app = RalfApplication(RalfConfig(deploy_mode="simpy"))
    env = simpy.Environment()

    sink = app.source(
        CounterSource(10),
        operator_config=OperatorConfig(
            simpy_config=SimpyOperatorConfig(
                shared_env=env, processing_time_s=0.01, stop_after_s=0.2
            )
        ),
    ).transform(
        Sum(),
        LIFO(),
        operator_config=OperatorConfig(
            simpy_config=SimpyOperatorConfig(shared_env=env, processing_time_s=0.1)
        ),
    )
    assert sink.config.simpy_config.shared_env is env

    app.deploy()

    env.run(1)

    record_trace = app.wait()
    assert record_trace[0].entry.request_id == 0

    request_ids = [r.entry.request_id for r in record_trace if "Sum" in r.entry.frame]
    assert request_ids == [0, 10, 19, 18, 17, 16, 15, 14, 13]
