import argparse
import json
import os
import random
import threading
import time

import msgpack
import numpy as np
import redis
import wandb
from kafka import KafkaProducer
from tqdm import tqdm

from ralf.client import RalfClient

client = RalfClient()


def make_kafka_producer():
    """
    Setup with:
    kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ralf
    """

    # serializer = lambda x: json.dumps(x).encode('utf-8')
    serializer = msgpack.dumps
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"], value_serializer=serializer
    )
    return producer


def make_redis_producer():
    r = redis.Redis()
    r.flushall()
    r.xgroup_create("ralf", "ralf-reader-group", mkstream=True)
    return r


def predict(event, model):
    """
    Calculate predicted residual and staleness (compared to last model timestamp) given event, model
    """
    # TODO: BE CAREFUL - changes based off timestamp units
    staleness = int(event["timestamp"] - model["timestamp"])  # / (60 * 60))
    last_trend = model["trend"]
    seasonal = model["seasonality"][staleness % len(model["seasonality"])]

    # calculate residual
    residual = event["value"] - last_trend - seasonal
    return residual, staleness


def dump_state(data, exp_dir):
    """
    Query models for all keys, and dump result and current event data in buffer
    """
    st = time.time()
    models = client.bulk_query(table_name="model")
    query_latency = time.time() - st

    # store key -> model mapping
    model_table = {}
    for m in models:
        model_table[int(m["key"])] = m

    staleness_time = []
    staleness_step = []
    for d in data:
        if d["key"] in model_table:
            staleness_time.append(d["send_time"] - model_table[d["key"]]["create_time"])
            staleness_step.append(d["timestamp"] - model_table[d["key"]]["timestamp"])

    # idenfity with last timestamp
    ts = max([d["timestamp"] for d in data])

    wandb.log({"query_latency": query_latency})
    wandb.log({"query_time": st})

    if len(staleness_time) > 0:
        avg_staleness_time = np.array(staleness_time).mean()
        avg_staleness_step = np.array(staleness_step).mean()
        wandb.log({"avg_staleness_time": avg_staleness_time})
        wandb.log({"avg_staleness_step": avg_staleness_step})

    # dump data
    open(os.path.join(exp_dir, f"models_ts_{ts}.json"), "w").write(
        json.dumps(
            {
                "models": model_table,
                "data": data,
                "query_time": st,
                "query_latency": query_latency,
            }
        )
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Specify experiment config")
    parser.add_argument("--num-keys", type=int, default=100)
    parser.add_argument("--send-rate", type=int, default=1000)
    parser.add_argument("--timesteps", type=int, default=1000)
    parser.add_argument("--slide", type=int)

    # Experiment related
    parser.add_argument(
        "--data-dir",
        type=str,
        default="/Users/sarahwooders/repos/flink-feature-flow/datasets",
    )
    parser.add_argument(
        "--exp-dir",
        type=str,
        default="/Users/sarahwooders/repos/flink-feature-flow/RayServer/experiments",
    )
    parser.add_argument("--file", type=str, default=None)
    parser.add_argument("--exp", type=str)  # experiment id
    args = parser.parse_args()
    exp_dir = os.path.join(args.exp_dir, args.exp)

    # experiment data
    wandb.init(project="stl", entity="ucb-ralf", group=args.exp)
    wandb.run.name = args.exp
    wandb.config["num_keys"] = args.num_keys
    wandb.config["send_rate"] = args.send_rate
    wandb.config["exp_dir"] = exp_dir
    wandb.config["slide"] = args.slide

    # producer = make_kafka_producer()
    producer = make_redis_producer()

    data = []
    last_ts = time.time()
    for i in tqdm(range(args.timesteps)):

        # sent (synthetic) events
        for key in range(1, args.num_keys + 1):
            value = {
                "send_time": time.time(),
                "key": key,
                "value": random.uniform(1, 10000),
                "timestamp": i,
            }
            # producer.send(
            #     "ralf",
            #     value=value,
            # )
            producer.xadd("ralf", value)
            data.append(value)
            time.sleep(1 / (args.send_rate * args.num_keys))

        if time.time() - last_ts > 60:
            # t = Timer(5.0, run_inference, [list(data)])
            # t.start()

            # query models
            print("run query", last_ts)
            thread = threading.Thread(target=dump_state, args=(data, exp_dir))
            thread.start()

            last_ts = time.time()
            data = []
