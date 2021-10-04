import json
import os
import re
import threading
import time
from glob import glob
from pathlib import Path
from typing import Dict

import msgpack
import pandas as pd
import redis
from absl import app, flags
from examples.stl.stl_offline_eval import main
from ralf.client import RalfClient

FLAGS = flags.FLAGS
flags.DEFINE_integer(
    "redis_model_db_id", default=2, help="Redis DB number for db snapshotting."
)
flags.DEFINE_integer(
    "redis_snapshot_interval_s", default=10, help="Interval for redis snapshotting."
)
flags.DEFINE_enum(
    "workload", "yahoo_csv", ["yahoo_csv"], "Type of the workload to send"
)
flags.DEFINE_integer(
    "send_rate_per_key", None, "records per seconds for each key", required=True
)
flags.DEFINE_string(
    "yahoo_csv_glob_path", None, "the glob pattern to match all files", required=True
)
flags.DEFINE_string(
    "yahoo_csv_key_extraction_regex",
    None,
    "extract regex from path name to int key",
)
flags.DEFINE_string(
    "experiment_dir", None, "directory to write metadata to", required=True
)
flags.DEFINE_string("experiment_id", None, "experiment run name", required=True)


client = RalfClient()


def make_redis_producer():
    r = redis.Redis()
    r.flushall()
    r.xgroup_create("ralf", "ralf-reader-group", mkstream=True)
    return r


def snapshot_db_state(pkl_path):
    r = redis.Redis(db=FLAGS.redis_model_db_id)
    state = {key: r.dumps(key) for key in r.keys("*")}
    with open(pkl_path, "wb") as f:
        msgpack.dump(state, f)


def _get_config() -> Dict:
    """Return all the flag vlaue defined here."""
    return {f.name: f.value for f in FLAGS.get_flags_for_module("__main__")}


def main(argv):
    print("using config", _get_config())
    exp_dir = Path(FLAGS.experiment_dir) / FLAGS.experiment_id
    exp_dir.mkdir(exist_ok=True, parents=True)
    with open(exp_dir / "client_config.json", "w") as f:
        json.dump(_get_config(), f)

    producer = make_redis_producer()
    csv_files = glob(FLAGS.yahoo_csv_glob_path)
    assert len(csv_files) > 0
    loaded_values = {
        int(
            re.match(
                FLAGS.yahoo_csv_key_extraction_regex, os.path.basename(path)
            ).group(1)
        ): (pd.read_csv(path)["value"])
        for path in csv_files
    }
    max_timestep = max(map(len, loaded_values.values()))
    print(f"Loaded {len(loaded_values)} files.")

    start_time = time.time()
    last_snapshot_time = time.time()
    for i in range(max_timestep):
        send_start = time.time()
        for key, time_series in loaded_values.items():
            if i < len(time_series):
                value = {
                    "send_time": time.time(),
                    "key": key,
                    "value": time_series[i],
                    "timestamp": i,
                }
                producer.xadd("ralf", value)
        send_duration = time.time() - send_start
        should_sleep = 1 / FLAGS.send_rate_per_key - send_duration
        # NOTE(simon): if we can't sleep this small, consider rewrite the client in rust.
        if should_sleep < 0:
            print(
                f"Cannot keep up the send rate {FLAGS.send_rate_per_key}, it took "
                f"{send_duration} to send all the keys."
            )
        else:
            time.sleep(should_sleep)

        if time.time() - last_snapshot_time > FLAGS.redis_snapshot_interval_s:
            snapshot_relative_time = time.time() - start_time
            last_snapshot_time = time.time()
            pkl_path = exp_dir / f"client_dump-{snapshot_relative_time}.pkl"
            print("dumping to ", pkl_path)
            thread = threading.Thread(target=snapshot_db_state, args=(pkl_path,))
            thread.start()


if __name__ == "__main__":
    app.run(main)
