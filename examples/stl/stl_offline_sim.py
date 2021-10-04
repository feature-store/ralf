import json
from pprint import pprint
from typing import Dict

import simpy
from absl import app, flags
from ralf.policies.load_shedding_policy import always_process, make_sampling_policy
from ralf.policies.processing_policy import fifo, lifo, make_sorter_with_key_weights
from ralf.simulation.mapper import RalfMapper, RoundRobinLoadBalancer
from ralf.simulation.priority_queue import PerKeyPriorityQueue
from ralf.simulation.source import Source
from ralf.simulation.window import WindowOperator

FLAGS = flags.FLAGS
prio_policies = {
    "fifo": fifo,
    "lifo": lifo,
    "query_aware": make_sorter_with_key_weights({i: i for i in range(101)}),
}
load_shed_policies = {
    "always_process": always_process,
    "sample_half": make_sampling_policy(0.5),
}


flags.DEFINE_enum(
    "key_prio_policy",
    "fifo",
    list(prio_policies.keys()),
    "The prioritization policy for a given key.",
)
flags.DEFINE_enum(
    "key_load_shed_policy",
    "always_process",
    list(load_shed_policies.keys()),
    "The load shedding policy for a given key.",
)
flags.DEFINE_integer(
    "per_key_records_per_second",
    100,
    "The send rate for each key.",
)
flags.DEFINE_integer("num_keys", 1, "The number of keys.")
flags.DEFINE_float("total_runtime_s", 14, "When to end the simulation.")
flags.DEFINE_float(
    "model_runtime_s",
    0.2,
    "The latency for the map function (when processing a single record).",
)
flags.DEFINE_integer("window_size", 24 * 7, "The sliding window size.")
flags.DEFINE_integer("slide_size", 4, "The sliding window slide size.")
flags.DEFINE_string(
    "source_data_path",
    None,
    "path to the csv file, if empty, sequential value will be used.",
)
flags.DEFINE_string(
    "output_path", None, "path to output json, if empty, print to stdout."
)


def _get_config() -> Dict:
    """Return all the flag vlaue defined here."""
    return {f.name: f.value for f in FLAGS.get_flags_for_module("__main__")}


def main(argv):
    env = simpy.Environment()
    # source --source_to_window_queue--> window --windows_to_mapper_queue--> mapper
    source_to_window_queue = simpy.Store(env)
    windows_to_mapper_queue = {
        i: PerKeyPriorityQueue(
            env,
            processing_policy=prio_policies[FLAGS.key_prio_policy],
            load_shedding_policy=load_shed_policies[FLAGS.key_load_shed_policy],
        )
        for i in range(FLAGS.num_keys)
    }
    Source(
        env,
        records_per_sec_per_key=FLAGS.per_key_records_per_second,
        num_keys=FLAGS.num_keys,
        next_queue=source_to_window_queue,
        total_run_time=FLAGS.total_runtime_s,
        data_file=FLAGS.source_data_path,
    )
    WindowOperator(
        env,
        window_size=FLAGS.window_size,
        slide_size=FLAGS.slide_size,
        source_queue=source_to_window_queue,
        next_queues=windows_to_mapper_queue,
    )
    m = RalfMapper(
        env,
        source_queues=windows_to_mapper_queue,
        model_run_time_s=FLAGS.model_runtime_s,
        # TODO(simon): customize this once we want different key selection policy
        key_selection_policy_cls=RoundRobinLoadBalancer,
    )
    env.run(until=FLAGS.total_runtime_s)

    plan = m.plan
    config = _get_config()
    if FLAGS.output_path:
        with open(FLAGS.output_path, "w") as f:
            json.dump(plan, f, indent=2)
        with open(FLAGS.output_path + ".config.json", "w") as f:
            json.dump(config, f, indent=2)
    else:
        pprint(plan)
        pprint(config)


if __name__ == "__main__":
    app.run(main)
