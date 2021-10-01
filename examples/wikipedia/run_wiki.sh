FILE="passages_sent_diffs_10010.pkl"
MODEL_FILE="/home/ubuntu/DPR/checkpoint/retriever/single/nq/bert-base-encoder.cp"
SEND_RATE=100
DATA_DIR="/home/ubuntu/flink-feature-flow/RayServer/data/"
EXP_DIR="/home/ubuntu/flink-feature-flow/RayServer/experiments/"
TIMESTAMP=$(date +%s)
EXP="experiment_$TIMESTAMP"
echo $EXP;
python wiki_server.py --data-dir $DATA_DIR --send-rate $SEND_RATE --exp-dir $EXP_DIR --exp $EXP --file $FILE --model_file $MODEL_FILE
#python wiki_client.py --exp $EXP_DIR
