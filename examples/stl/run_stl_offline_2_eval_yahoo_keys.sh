set -ex

data_dir="/home/ubuntu/ydata-labeled-time-series-anomalies-v1_0/A4Benchmark/"

tmp_script=`mktemp`
for data in `ls $data_dir/A4Benchmark-TS*`
do
    key=`basename $data`
    for slide in 6 12 18 24 48 96 168 192 336 672
    do
        echo python stl_offline_eval.py --offline-yahoo-csv-path $data \
            --offline-plan-path ./result/offline_1_slide/plan/slide_${slide}_plan.json \
            --output-path ./result/offline_1_slide/plan_eval/slide_${slide}_key_${key} >> $tmp_script
    done
done

cat $tmp_script | parallel --bar bash -l -c