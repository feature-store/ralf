import threading

import event_metrics
import structlog


def get_logger():
    structlog.configure(
        processors=[
            structlog.threadlocal.merge_threadlocal,
            structlog.processors.CallsiteParameterAdder(
                [
                    structlog.processors.CallsiteParameter.FILENAME,
                    structlog.processors.CallsiteParameter.FUNC_NAME,
                    structlog.processors.CallsiteParameter.LINENO,
                ]
            ),
        ]
        + structlog.get_config()["processors"]
    )
    return structlog.get_logger()


thread_local_storage = threading.local()


def set_metrics_conn(metrics_conn: event_metrics.MetricConnection):
    global thread_local_storage
    thread_local_storage.metrics_conn = metrics_conn


def get_metrics_conn() -> event_metrics.MetricConnection:
    return thread_local_storage.metrics_conn


MERGE_DB_SCRIPTS = """
set -ex

rm -f merged.db

for path in *.db
do
  sqlite3 ${path} '.dump' | sed -e 's/CREATE TABLE/CREATE TABLE IF NOT EXISTS/g' | sqlite3 merged.db
done

cat <<'EOF' >> merge_to_parquet.py
from tqdm import tqdm
import pandas as pd
import sqlite3
import json

tqdm.pandas()

df = pd.read_sql_query("select * from metrics", sqlite3.connect("merged.db"))

label_df = df["labels_json"].progress_apply(lambda v: json.loads(v))
label_df = pd.DataFrame(label_df.tolist())
merged_df = pd.concat([df, label_df], axis=1)
merged_df = merged_df.drop(columns=["labels_json"])

for col_name, dtype in merged_df.dtypes.iteritems():
    if dtype.name == "object":
        merged_df[col_name] = merged_df[col_name].astype("category")

merged_df.to_parquet("merged.pq", compression=None)
EOF

python merge_to_parquet.py
"""
