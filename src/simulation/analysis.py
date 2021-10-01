"Quick EDA script to figure out the shape of yahoo dataset"
import glob
import sys

import numpy as np
import pandas as pd

path = "/home/ubuntu/ydata-labeled-time-series-anomalies-v1_0/A1Benchmark"
csvs = glob.glob(path + "/*.csv")

dfs = [pd.read_csv(path) for path in csvs]
max_length = max(map(len, dfs))
min_length = min(map(len, dfs))
print("min", min_length, "max", max_length)

# Plot a histogram of anomaly distribution over time.
is_anomalies = [df["is_anomaly"].values.nonzero() for df in dfs]
mat = np.stack(
    [np.histogram(i, bins=20, range=(0, max_length))[0] for i in is_anomalies]
)
with np.printoptions(threshold=sys.maxsize):
    print(mat)
