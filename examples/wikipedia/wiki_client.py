import sys
from tqdm import tqdm
import argparse
import os
import json
import time

from threading import Timer

import psutil

from ralf.client import RalfClient

client = RalfClient()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Specify experiment config")

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
    args = parser.parse_args()

    doc_id = "56485450"
    res = client.point_query(key=doc_id, table_name="doc_embedding")
    print(res["pass_embeddings"])
    res = client.bulk_query(table_name="doc_embedding")
    print([r["pass_embeddings"] for r in res])
