# For keys=10000 - connection failed?

NUM_KEYS=100
SEND_RATE=100
WINDOW=672
SEASONALITY=168
#FILENAME="logs-k$NUM_KEYS-r$SEND_RATE-t$TS_w$WINDOW-s$SEASONALITY.txt"
FILE="A4Benchmark-TS3.csv"
DATA_DIR="/home/ubuntu/ydata-labeled-time-series-anomalies-v1_0/A4Benchmark/"
EXP_DIR="/home/ubuntu/flink-feature-flow/RayServer/experiments/"
SLIDE_SIZE_PLAN_PATH="/home/ubuntu/flink-feature-flow/analysis/notebooks/sample_lp_plan.json"

redis-server --save "" --appendonly no &

TIMESTAMP=$(date +%s)
EXP="experiment_$TIMESTAMP"
SLIDE=672

#TODO(simon): using gflags for experiment management
# https://abseil.io/docs/python/guides/flags
python stl_server.py \
  --data-dir $DATA_DIR --exp-dir $EXP_DIR --exp $EXP \
  --num-keys $NUM_KEYS --send-rate $SEND_RATE \
  --window $WINDOW --seasonality $SEASONALITY --slide $SLIDE \
  --per-key-slide-size-plan $SLIDE_SIZE_PLAN_PATH &

python stl_client.py --exp $EXP --exp-dir $EXP_DIR --num-keys $NUM_KEYS --send-rate $SEND_RATE --timesteps 10000 --slide $SLIDE
# Ray stop should also stop that redis-server
ray stop --force
