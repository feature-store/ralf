set -ex

data_dir="/home/ubuntu/ydata-labeled-time-series-anomalies-v1_0/A4Benchmark/"

tmp_script=`mktemp`
for data in `ls $data_dir/A4Benchmark-TS*`
do
    key=`basename $data`
    echo python stl_offline_eval.py --offline-yahoo-csv-path $data \
        --offline-run-oracle true \
        --output-path ./result/offline_1_slide/plan_eval/oracle_key_${key} >> $tmp_script
done

cat $tmp_script | parallel --bar bash -l -c