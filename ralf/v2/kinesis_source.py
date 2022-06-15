from ralf.v2 import BaseTransform, Record
from typing import Deque, Dict, Iterable, List, Optional, Type, Union

class KinesisDataSource(BaseTransform): 

    def __init__(self, stream_name: str, stream_arn: str, num_shards: int, prefix="ralf-kinesis"): 

        self.stream_name = stream_name
        self.stream_arn = stream_arn

        # read operator shard index
        self.shard_idx = self._operator.context["shard_idx"]
        self.num_replicas = self._operator.context["num_replicas"]

        # consumer name
        self.name = f"{prefix}-consumer-{self.shard_id}"

        # shared queue of data
        self.queue = queue.Queue(maxsize=10000)

        # kinesis shards to manage
        self.shards = []

    def prepare(self): 

        # list shards 
        shards = [] 
        next_token = None
        while True:
            response = self.client.list_shards(StreamName=self.stream_name, MaxResults=10, NextToken=next_token)
            if len(response["Shards"]) == 0: 
                break

            shards += response["Shards"]
            next_token = response["NextToken"]

        assert len(shards) == num_shards

        shards = sorted(shards)
        for i in range(len(shards)):
            if i % self.num_replicas == self.shard_idx: 
                self.shards.append(shards[i])

        # check existing consumers
        for consumer in response["Consumers"]:
            consumer_arn = consumer["ConsumerARN"]
            consumer_name = consumer["ConsumerName"]
            if consumer_name == self.name:
                response = client.deregister_stream_consumer(
                    StreamARN=self.stream_arn,
                    ConsumerName=consumer_name,
                    ConsumerARN=consumer_arn
                )
                status = "DELETING"
                while status == "DELETING": 
                    try:
                        response = client.describe_stream_consumer(
                            StreamARN=stream_arn,
                            ConsumerName=consumer_name,
                            ConsumerARN=consumer_arn
                        )
                        status = response["ConsumerDescription"]["ConsumerStatus"]
                    except Exception as e:
                        print(e)
                print(f"Deregistered {consumer_name}")

        # create stream consumer
        response = client.register_stream_consumer(
            StreamARN=stream_arn,
            ConsumerName=self.name
        )
        consumer_name = response["Consumer"]["ConsumerName"] 
        consumer_arn = response["Consumer"]["ConsumerARN"]

        status = None
        while status != "ACTIVE": 
            response = client.describe_stream_consumer(
                StreamARN=stream_arn,
                ConsumerName=consumer_name,
                ConsumerARN=consumer_arn
            )
            status = response["ConsumerDescription"]["ConsumerStatus"]

        print("Finished creating stream consumer")
        for shard in self.shards: 


            def listen_to_shard(target_shard):
                shard_id = target_shard["ShardId"]
                seq_no = target_shard['SequenceNumberRange']['StartingSequenceNumber']
                while True:

                    # note: only lasts for 5 min then needs to be re-set
                    response = client.subscribe_to_shard(
                        ConsumerARN=consumer_arn,
                        ShardId=shard_id,
                        StartingPosition={
                            'Type': 'AT_SEQUENCE_NUMBER',
                            'SequenceNumber': shard_id,
                        }
                    )

                    event_stream = response["EventStream"]
                    for record in event_stream["SubscribeToShardEvent"]["Records"]:
                        seq_no = record["SequenceNumber"]
                        arrive_ts = record["ApproximateArrivalTimestamp"]
                        data = record["Data"]
                        data = json.loads(data.decode('utf8').replace("'", '"'))
                        data["arrival_ts"] = arrive_ts

                        # TODO: somehow prevent queue overflow
                        self.queue.push(data)

            # assign thread to listen to shard
            thread = threading.Thread(listen_to_shard, shard)
            thread.start()
            print("Listeining", shard)
        

    def on_event(self, record: Record) -> Union[None, Record, Iterable[Record]]:
        # return data from shards

        if self.queue.empty(): return None

        records = []
        while len(records) < self.buffer_size and not self.queue.empty(): 
            records.append(queue.pop())

        return records
 
