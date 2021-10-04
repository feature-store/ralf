set -xe

TIMESTAMP=$(date +%s)
EXP_DIR="./result/online_1_slide"
EXP="experiment_$TIMESTAMP"

# Turn off redis dump
redis-server --save "" --appendonly no &

python stl_online_server.py \
  --experiment_dir $EXP_DIR --experiment_id $EXP \
  --global_slide_size 48 --per_key_slide_size_plan_path "./result/offline_1_slide/min_loss_plan.json" \
  --seasonality 168 \
  --window_size 672 &

python stl_online_client.py \
  --experiment_dir $EXP_DIR --experiment_id $EXP \
  --redis_snapshot_interval_s 5 --send_rate_per_key 10 \
  --workload yahoo_csv \
  --yahoo_csv_glob_path '/home/ubuntu/ydata-labeled-time-series-anomalies-v1_0/A4Benchmark/A4Benchmark-TS*.csv' \
  --yahoo_csv_key_extraction_regex 'A4Benchmark-TS(\d+).csv'

# Ray stop should also stop that redis-server
ray stop --force
