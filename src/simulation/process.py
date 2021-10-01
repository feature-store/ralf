import json
import sys
from typing import Optional

import simpy
from ralf.load_shedding_policy import (
    always_process,
    make_mean_policy,
    make_sampling_policy,
)
from ralf.processing_policy import fifo, lifo, make_sorter_with_key_weights
from ralf.simulation.priority_queue import PerKeyPriorityQueue
from ralf.simulation.source import Source
from ralf.simulation.window import WindowOperator
from ralf.simulation.mapper import RalfMapper, RoundRobinLoadBalancer


policies = {
    "fifo": fifo,
    "lifo": lifo,
    "always_process": always_process,
    "sample_half": make_sampling_policy(0.5),
    "changing_mean": make_mean_policy(0.1),
}


def run_once(
    prioritization_policy: str,
    load_sheeding_policy: str,
    per_key_records_per_second: int = 100,
    num_keys: int = 1,
    total_runtime_s: float = 14,
    model_runtime_s: float = 0.2,
    window_size: int = 24 * 7,
    slide_size: int = 4,
    key: Optional[int] = None,
):

    env = simpy.Environment()
    # source --source_to_window_queue--> window --windows_to_mapper_queue--> mapper
    source_to_window_queue = simpy.Store(env)
    windows_to_mapper_queue = {
        i: PerKeyPriorityQueue(
            env,
            processing_policy=policies[prioritization_policy],
            load_shedding_policy=policies[load_sheeding_policy],
        )
        for i in range(num_keys)
    }
    s = Source(
        env,
        records_per_sec_per_key=per_key_records_per_second,
        num_keys=num_keys,
        next_queue=source_to_window_queue,
        total_run_time=total_runtime_s,
        data_file=f"/home/ubuntu/flink-feature-flow/RayServer/data/yahoo/A4/{key}.csv"
        if key is not None
        else None,
    )
    w = WindowOperator(
        env,
        window_size=window_size,
        slide_size=slide_size,
        source_queue=source_to_window_queue,
        # TODO(simon): perform sharding to queue either here or in a mega operator.
        next_queues=windows_to_mapper_queue,
    )
    m = RalfMapper(
        env,
        source_queues=windows_to_mapper_queue,
        model_run_time_s=model_runtime_s,
        key_selection_policy_cls=RoundRobinLoadBalancer,
    )
    env.run(until=total_runtime_s)

    plan = m.ready_time_to_batch
    return plan


if __name__ == "__main__":
    sample_run = True

    # Try out a policy where lower id keys are priortize over higher id keys.
    policies["query_aware"] = make_sorter_with_key_weights({i: i for i in range(101)})

    prioritization_policies = ["quary_aware"]  # ["fifo", "lifo"]
    load_shedding_policies = ["always_process"]
    num_keys = [100]  # [1]
    window_sizes = [672]
    slide_sizes = [12]  # [1, 6, 12, 18, 24, 48, 96, 168, 192, 336, 672]
    model_runtimes = [0.001]
    records_per_second = [100]

    for prio_policy in prioritization_policies:
        for load_shed_policy in load_shedding_policies:
            for keys in num_keys:
                for window in window_sizes:
                    for slide in slide_sizes:
                        for runtime in model_runtimes:
                            for rate in records_per_second:
                                out_path = f"./plan-{prio_policy}-{load_shed_policy}-{keys}-{window}-{slide}-{runtime}-{rate}.json"
                                print("running", out_path)
                                plan = run_once(
                                    "query_aware",  # prio_policy,
                                    load_shed_policy,
                                    rate,
                                    keys,
                                    total_runtime_s=16,
                                    model_runtime_s=0.1,  # runtime,
                                    window_size=window,
                                    slide_size=slide,
                                )
                                with open(out_path, "w") as f:
                                    json.dump(plan, f)

                                if sample_run:
                                    sys.exit(0)

                                # This is for key-specific policies (where the policy needs to see they keys' data)
                                # for key in range(1, 100, 1):

                                #    out_path = f"/home/ubuntu/flink-feature-flow/RayServer/plans/plan-{prio_policy}-{load_shed_policy}-{keys}-{window}-{slide}-{runtime}-{rate}_{key}.json"
                                #    print("running", out_path)
                                #    run_once(
                                #        out_path,
                                #        prio_policy,
                                #        load_shed_policy,
                                #        rate,
                                #        keys,
                                #        total_runtime_s = 16,
                                #        model_runtime_s = runtime,
                                #        window_size = window,
                                #        slide_size = slide,
                                #        key=key,
                                #    )
