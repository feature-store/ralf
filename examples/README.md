## Experiment Tracking 
Experiments can be tracked in W&B at `https://wandb.ai/ucb-ralf/stl`. The experiment scripts will also create a directory in `experiments/` which will log the following for each experiment 
* `snapshots.jsonl` (snapshot JSONs of the server) 
* `metadata.json` (experiment metadata)
* `models_ts_{TIMESTAMP}.json` (query results at a timestamp)

## Example Workload Setup
### Time Series Decomposition (Yahoo/STL)
* Download yahoo anomaly detection dataset from: `https://feature-store-datasets.s3.us-west-2.amazonaws.com/yahoo/dataset.tgz`
  - Each CSV file `A3Benchmark-TS{KEY}.csv` is for a different time series 
  - Each point is sampled hourly, and the data has hourly, monthly, and weekly seasonality
* Create kafka topic`bin/kafka-topics.sh --create --topic ralf --bootstrap-server localhost:9092`
* Run server `bash run_stl.sh`
  - Change the slide size to vary the frequency of re-computing 
  - Keep window size the same across experiments (lower window sizes will be cheap to fix, and larger window sizes become much more expensive)
  - Ideally the window size is a multiple of the seasonality length, which in this case is 24 (hourly) or 168 (weekly) - Splunk recommended window size = 4x the seasonality
* Clear out kafka topic with `bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic ralf`

## Wikipedia / Document Embedding
Download data (for one passage): 
```
aws s3 cp s3://feature-store-datasets/wikipedia/diffs/passages/passages_sent_diffs_10010.pkl # one file
aws s3 cp s3://feature-store-datasets/wikipedia/diffs/passages/* # all files
```
Download model: 
```
aws s3 cp s3://feature-store-datasets/wikipedia/models/bert-base-encoder.cp
```
Run `pip3 install -e /home/ubuntu/DPR`
* Run script `run_wiki.sh` (currently not using kafka) 

