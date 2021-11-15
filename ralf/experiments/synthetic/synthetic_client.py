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
    record = client.point_query(key=1, table_name="sink")
    print(f"{record['key']} -> {record['value']}: {time.time() - record['create_time']}")