import json
import os
import argparse
from typing import List
import time
import numpy as np
import pickle
from collections import defaultdict

from typing import Optional, List, Type
import ray

import torch
from torch import nn


import pandas as pd

import psutil

import sys
from kafka import KafkaConsumer
import msgpack

from ralf.operator import Operator, DEFAULT_STATE_CACHE_SIZE
from ralf.operators import (
    Source,
    TumblingWindow,
    LeftJoin,
)
from ralf.state import Record, Schema
from ralf.core import Ralf
from ralf.table import Table

from dpr.models import init_biencoder_components
from dpr.options import (
    add_encoder_params,
    setup_args_gpu,
    print_args,
    set_encoder_params_from_state,
    add_tokenizer_params,
    add_cuda_params,
)
from dpr.utils.model_utils import (
    setup_for_distributed_mode,
    load_states_from_checkpoint,
    get_model_obj,
    move_to_device,
)
from dpr.utils.data_utils import Tensorizer


@ray.remote
class EditSource(Source):

    """Read in pre-processed passages from wikipedia documents.

    Each edits provides a list of of passage ids, texts, diffs, and diff type.
    """

    def __init__(
        self,
        send_rate,
        f,
        cache_size=DEFAULT_STATE_CACHE_SIZE,
    ):
        schema = Schema(
            "key",
            {
                "key": str,
                "doc_id": str,
                "pass_ids": List[str],
                "pass_texts": List[str],
                "diffs_a": List[List[str]],  # removed text
                "diffs_b": List[List[str]],  # added text
                "diff_types": List[
                    str
                ],  # CREATE (new passage), EDIT (modified passage)
                "create_time": float,
                "send_time": float,
                "timestamp": int,
            },
        )

        super().__init__(schema, cache_size, num_worker_threads=1)

        print("Reading pickle", f)
        self.data = pickle.loads(open(f, "rb").read())
        self.send_rate = send_rate
        self.ts = 0

    def next(self):
        try:
            if self.ts < len(self.data):
                d = self.data[self.ts]
                t = time.time()
                record = Record(
                    key=d["doc_id"],
                    doc_id=d["doc_id"],
                    pass_ids=d["pass_ids"],
                    pass_texts=d["pass_texts"],
                    diffs_a=d["diffs_a"],
                    diffs_b=d["diffs_b"],
                    diff_types=d["diff_types"],
                    create_time=t,
                    send_time=t,
                    timestamp=self.ts,
                )
                self.ts += 1
                time.sleep(1 / self.send_rate)
                return [record]
            else:
                print("STOP ITERATION", self.ts)
        except Exception as e:
            print(e)
            raise StopIteration


@ray.remote
class Passages(Operator):
    def __init__(
        self,
        cache_size=DEFAULT_STATE_CACHE_SIZE,
        lazy=False,
        num_worker_threads=1,
    ):

        schema = Schema(
            "key",
            {
                "key": str,
                "doc_id": str,
                "pass_id": str,
                "pass_text": str,
                "diffs_a": List[str],
                "diffs_b": List[str],
                "diff_type": str,
                "text": str,
                "timestamp": int,
                "create_time": float,
                "send_time": float,
            },
        )
        super().__init__(schema, cache_size, lazy, num_worker_threads)

    def on_record(self, record: Record) -> Record:

        try:
            records = []
            for i in range(len(record.pass_ids)):
                records.append(
                    Record(
                        key=record.pass_ids[i],
                        pass_id=record.pass_ids[i],
                        pass_text=record.pass_texts[i],
                        diffs_a=record.diffs_a[i],
                        diffs_b=record.diffs_b[i],
                        diff_type=record.diff_types[i],
                        doc_id=record.key,
                        timestamp=record.timestamp,
                        create_time=record.create_time,
                        send_time=record.send_time,
                    )
                )
            # print("passage", len(records))
            return records
        except Exception as e:
            print(e)


@ray.remote
class Retriever(Operator):
    def __init__(
        self,
        args,
        cache_size=DEFAULT_STATE_CACHE_SIZE,
        lazy=False,
        num_worker_threads=4,
    ):

        schema = Schema(
            "key",
            {
                "key": str,
                "pass_id": str,
                "pass_embedding": np.array,  # TODO: change to tensor,
                "pass_text": str,
                "doc_id": str,
                "timestamp": int,
                "create_time": float,
                "send_time": float,
            },
        )
        super().__init__(schema, cache_size, lazy, num_worker_threads)

        saved_state = load_states_from_checkpoint(args.model_file)
        set_encoder_params_from_state(saved_state.encoder_params, args)
        print(args)

        self.tensorizer, self.encoder, _ = init_biencoder_components(
            args.encoder_model_type, args, inference_only=True
        )

        self.encoder = self.encoder.ctx_model

        self.encoder, _ = setup_for_distributed_mode(
            self.encoder,
            None,
            args.device,
            args.n_gpu,
            args.local_rank,
            args.fp16,
            args.fp16_opt_level,
        )
        self.encoder.eval()

        model_to_load = get_model_obj(self.encoder)

        prefix_len = len("ctx_model.")
        ctx_state = {
            key[prefix_len:]: value
            for (key, value) in saved_state.model_dict.items()
            if key.startswith("ctx_model.")
        }
        model_to_load.load_state_dict(ctx_state)
        self.device = args.device

    def on_record(self, record: Record) -> Record:

        try:
            st = time.time()
            batch_token_tensors = [self.tensorizer.text_to_tensor(record.pass_text)]

            ctx_ids_batch = move_to_device(
                torch.stack(batch_token_tensors, dim=0), self.device
            )
            ctx_seg_batch = move_to_device(torch.zeros_like(ctx_ids_batch), self.device)
            ctx_attn_mask = move_to_device(
                self.tensorizer.get_attn_mask(ctx_ids_batch), self.device
            )
            with torch.no_grad():
                _, embedding, _ = self.encoder(
                    ctx_ids_batch, ctx_seg_batch, ctx_attn_mask
                )
            embedding = embedding.cpu().numpy()

            record = Record(
                key=record.key,
                pass_id=record.pass_id,
                pass_embedding=embedding,
                pass_text=record.pass_text,
                doc_id=record.doc_id,
                timestamp=record.timestamp,
                create_time=record.create_time,
                send_time=record.send_time,
            )
            return record
        except Exception as e:
            print(e)


@ray.remote
class GroupByDoc(Operator):
    def __init__(
        self,
        cache_size=DEFAULT_STATE_CACHE_SIZE,
        lazy=False,
        num_worker_threads=1,
    ):

        schema = Schema(
            "key",
            {
                "key": str,
                "doc_id": str,
                "pass_embeddings": List[np.array],
                "pass_ids": List[str],
                "pass_texts": List[str],
                "timestamp": int,
                "create_time": float,
                "send_time": float,
            },
        )
        super().__init__(schema, cache_size, lazy, num_worker_threads)

        self.docs = {}

    def on_record(self, record: Record) -> Record:
        try:
            if record.doc_id not in self.docs:
                doc = {"pass_ids": [], "pass_embeddings": [], "pass_texts": []}
            else:
                doc = self.docs[record.doc_id]
            if record.pass_id in doc["pass_ids"]:
                index = doc["pass_ids"].index(record.pass_id)
                doc["pass_embeddings"][index] = record.pass_embedding
                doc["pass_texts"][index] = record.pass_text
            else:
                doc["pass_embeddings"].append(record.pass_embedding)
                doc["pass_texts"].append(record.pass_text)
                doc["pass_ids"].append(record.pass_id)

            self.docs[record.doc_id] = doc

            return Record(
                key=record.doc_id,
                doc_id=record.doc_id,
                pass_embeddings=doc["pass_embeddings"],
                pass_ids=doc["pass_ids"],
                pass_texts=doc["pass_texts"],
                timestamp=record.timestamp,
                create_time=record.create_time,
                send_time=record.send_time,
            )
        except Exception as e:
            print(e)


def from_file(send_rate: int, f: str):
    return Table([], EditSource, send_rate, f)


def from_kafka(topic: str):
    return Table([], KafkaSource, topic)


def write_metadata(args, ex_dir):
    # write experiment metadata
    metadata = {"pipeline": "wikipedia"}
    metadata["send_rate"] = args.send_rate
    metadata["file"] = args.file
    open(os.path.join(ex_dir, "metadata.json"), "w").write(json.dumps(metadata))


def create_doc_pipeline(args):

    # create Ralf instance
    # ralf_conn = Ralf(metric_dir=os.path.join(args.exp_dir, args.exp))
    ralf_conn = Ralf(
        metric_dir=os.path.join(args.exp_dir, args.exp), log_wandb=True, exp_id=args.exp
    )

    # create pipeline
    source = from_file(args.send_rate, os.path.join(args.data_dir, args.file))
    passages = source.map(Passages).as_queryable("passages")
    pass_embeddings = passages.map(Retriever, args, num_replicas=8).as_queryable(
        "pass_embedding"
    )
    doc_embeddings = pass_embeddings.map(GroupByDoc).as_queryable("doc_embedding")

    # deploy
    ralf_conn.deploy(source, "source")

    return ralf_conn


def main():

    parser = argparse.ArgumentParser(description="Specify experiment config")
    parser.add_argument("--send-rate", type=int, default=100)
    parser.add_argument("--timesteps", type=int, default=10)

    # Experiment related
    # TODO: add wikipedia dataset
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

    add_encoder_params(parser)
    add_tokenizer_params(parser)
    add_cuda_params(parser)

    parser.add_argument("--file", type=str, default=None)
    parser.add_argument("--exp", type=str)  # experiment id
    args = parser.parse_args()
    print(args)

    assert args.model_file, "Use --model_file to initialize encoding model"

    setup_args_gpu(args)

    # create experiment directory
    ex_id = args.exp
    ex_dir = os.path.join(args.exp_dir, ex_id)
    os.mkdir(ex_dir)
    write_metadata(args, os.path.join(args.exp_dir, args.exp))

    # create stl pipeline
    ralf_conn = create_doc_pipeline(args)
    ralf_conn.run()

    # snapshot stats
    run_duration = 120
    snapshot_interval = 10
    start = time.time()
    while time.time() - start < run_duration:
        snapshot_time = ralf_conn.snapshot()
        remaining_time = snapshot_interval - snapshot_time
        if remaining_time < 0:
            print(
                f"snapshot interval is {snapshot_interval} but it took {snapshot_time} to perform it!"
            )
            time.sleep(0)
        else:
            print("writing snapshot", snapshot_time)
            time.sleep(remaining_time)


if __name__ == "__main__":
    main()
