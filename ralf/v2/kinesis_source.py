from ralf.v2 import BaseTransform, Record
import json
import time
import boto3
import queue
from dacite import from_dict
from typing import Deque, Dict, Iterable, List, Optional, Type, Union

class KinesisDataSource(BaseTransform): 

    def __init__(self, stream_name: str, stream_arn: str, num_shards: int, shard_key, data_class, prefix="ralf-kinesis"): 

        print("init source")
        self.data_class = data_class
        self.shard_key = shard_key
        self.prefix = prefix
        self.stream_name = stream_name
        self.stream_arn = stream_arn

        self.num_shards = num_shards

        # kinesis shards to manage
        self.shards = []

        self.index = 0


    def prepare(self): 

        # read operator shard index
        self.shard_idx = self._operator.context["shard_idx"]
        self.num_replicas = self._operator.context["num_replicas"]

        # consumer name
        self.name = f"{self.prefix}-consumer-{self.shard_idx}"

        self.client = boto3.client('kinesis')

        # list shards 
        shards = [] 
        next_token = None
        while True:
            if next_token is None:
                response = self.client.list_shards(StreamName=self.stream_name, MaxResults=10)
            else: 
                response = self.client.list_shards(MaxResults=10, NextToken=next_token)

            if len(response["Shards"]) == 0: 
                break

            shards += response["Shards"]
            if "NextToken" not in response:
                break
            next_token = response["NextToken"]

        assert len(shards) == self.num_shards
        
        # check existing consumers
        response = self.client.list_stream_consumers(StreamARN=self.stream_arn)
        for consumer in response["Consumers"]:
            consumer_arn = consumer["ConsumerARN"]
            consumer_name = consumer["ConsumerName"]
            if consumer_name == self.name:
                response = self.client.deregister_stream_consumer(
                    StreamARN=self.stream_arn,
                    ConsumerName=consumer_name,
                    ConsumerARN=consumer_arn
                )
                status = "DELETING"
                while status == "DELETING": 
                    try:
                        response = self.client.describe_stream_consumer(
                            StreamARN=self.stream_arn,
                            ConsumerName=consumer_name,
                            ConsumerARN=consumer_arn
                        )
                        status = response["ConsumerDescription"]["ConsumerStatus"]
                    except Exception as e:
                        print(e)
                        break
                print(f"Deregistered {consumer_name}")

        # create stream consumer
        response = self.client.register_stream_consumer(
            StreamARN=self.stream_arn,
            ConsumerName=self.name
        )
        consumer_name = response["Consumer"]["ConsumerName"] 
        consumer_arn = response["Consumer"]["ConsumerARN"]

        status = None
        while status != "ACTIVE": 
            response = self.client.describe_stream_consumer(
                StreamARN=self.stream_arn,
                ConsumerName=consumer_name,
                ConsumerARN=consumer_arn
            )
            status = response["ConsumerDescription"]["ConsumerStatus"]
            time.sleep(2)

        print("Finished creating stream consumer")

        # mark shards
        shards = sorted(shards, key=lambda d: d["ShardId"])
        for i in range(len(shards)):
            if i % self.num_replicas == self.shard_idx: 
                self.shards.append({
                    "shard_id": shards[i]["ShardId"],
                    "seq_no": shards[i]['SequenceNumberRange']['StartingSequenceNumber']
                })

        print(self.shards)


    def get_shard_events(self): 
         # return data from shards
        i = self.index % len(self.shards)
        shard_id = self.shards[i]["shard_id"]
        seq_no = self.shards[i]["seq_no"]
        self.index += 1


        shard_iter = self.client.get_shard_iterator(
            StreamName=self.stream_name,
	    ShardId=shard_id,
	    ShardIteratorType='AFTER_SEQUENCE_NUMBER', # or at?
	    StartingSequenceNumber=seq_no,
	)["ShardIterator"]


        # get data from shard
        records = []
        response = self.client.get_records(ShardIterator=shard_iter, Limit=100)
        for record in response["Records"]:
            seq_no = record["SequenceNumber"]
            arrive_ts = record["ApproximateArrivalTimestamp"]
            data = record["Data"]
            data = json.loads(json.loads(data.decode('utf8').replace("'", '"')))
            data["ingest_time"] = arrive_ts
            records.append(data)

        # update query sequence no
        self.shards[i]["seq_no"] = seq_no
        print(seq_no)

        return records
        

    def on_event(self, record: Record) -> Union[None, Record, Iterable[Record]]:
        events = self.get_shard_events()

        records = []
        for e in events: 
            try:
                records.append(Record(
                    entry=self.data_class(**e),
                    shard_key=str(e[self.shard_key])
                ))
            except Exception as error:
                raise ValueError(error)
        print(f"Sending {len(records)} records")
        return records
