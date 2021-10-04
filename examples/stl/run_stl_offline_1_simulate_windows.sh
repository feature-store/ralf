set -xe

for slide in 1 6 12 18 24 48 96 168 192 336 672
do
    python stl_offline_sim.py --model_runtime_s 0 --total_runtime_s 2000 --per_key_records_per_second 1 \
        --window_size 672 --slide_size ${slide} --output_path result/offline_1_slide/plan/slide_${slide}_plan.json
done
