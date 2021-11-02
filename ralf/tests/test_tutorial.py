#!/usr/bin/env python
# coding: utf-8

# # Introduction
# This tutorial will go through how to setup a featurization pipeline in `ralf`. We'll setup a pipeline for computing user features given a data stream of user ratings. We'll then query the user features to predict the rating a user will give a movie.
#
# To do so, we'll do the following:
# 1. Create feature tables from the movie lens dataset which are incrementally maintained by `ralf`
# 2. Create a ralf client which queries the feature tables
# 3. Implement load shedding policies to reduce feature computation cost

# # Creating a featurization pipeline
# We create a instance of ralf to that we can start creating tables.

# In[ ]:

from ralf import Ralf

ralf_server = Ralf()


# ### Creating Source Tables
# Source tables define the raw data sources that are run through ralf to become features. `ralf` lets you create both static batch (e.g. from a CSV) and dynamic streaming sources (e.g. from Kafka).
#
# To define a source, we implement a `SourceOperator`.

# In[ ]:

from ralf.operators.source import SourceOperator
from ralf import Record
import random
import time


class RatingsSource(SourceOperator):
    def __init__(self, schema, kafka_topic):
        self.topic = kafka_topic

        super().__init__(schema)

    def next(self):
        time.sleep(0.01)
        user_id = random.randint(1, 10)
        movie_id = random.randint(100, 200)
        rating = random.randint(1, 5)
        return [Record(user=str(user_id), movie=movie_id, rating=rating)]


# We specify a schema using ralf's `Schema` object.

# In[ ]:


from ralf import Schema

source_schema = Schema(
    primary_key="user", columns={"user": str, "movie": int, "rating": float}
)


# We can now add the source to our ralf instance.

# In[ ]:


source = ralf_server.create_source(RatingsSource, args=(source_schema, "ratings_topic"))


# ### Creating Feature Tables
# Now that we have data streaming into ralf through the source table, we can define derived feature tables from the source table.
#
# Feature tables follow an API similar to pandas dataframes. We define feature tables in terms of 1-2 parent tables and an operator which specifies how to transform parent data.
#
#
# For example, we can calculate the average rating for each user with an `AverageRating` operator:

# In[ ]:

from collections import defaultdict
import numpy as np

from ralf import Operator, Record


class AverageRating(Operator):
    def __init__(self, schema):
        self.user_ratings = defaultdict(list)

        super().__init__(schema)

    def on_record(self, record: Record):
        self.user_ratings[record.user].append(record.rating)
        ratings = np.array(self.user_ratings[record.user])
        output_record = Record(user=record.user, average=ratings.mean())
        return output_record


# The `AverageRating` operator can be used to define a feature table containing the average rating for each user.

# In[ ]:

from ralf import Schema

average_rating_schema = Schema(
    primary_key="user", columns={"user": str, "average": float}
)
average_rating = source.map(AverageRating, args=(average_rating_schema,))


# ### Adding Processing Policies
# In many cases, we may only need to sub-sample some of the data to get the features we need. We can add a simple load shedding policy to the `average_rating` table.

# In[ ]:

from ralf import LoadSheddingPolicy, Record
import random


class SampleHalf(LoadSheddingPolicy):
    def process(self, candidate_record: Record, current_record: Record) -> bool:
        return random.random() < 0.5


average_rating.add_load_shedding(SampleHalf)
average_rating.as_queryable("average")


# ## Creating a `ralf` Client
# Now that we have a simple pipeline, we can query the ralf server for features.
# To do that, let's kick off the pipeline

ralf_server.run()

# In[ ]:
from ralf import RalfClient

ralf_client = RalfClient()


# In[ ]:


ralf_client.point_query(table_name="average", key=1)


# In[ ]:


ralf_client.bulk_query(table_name="average")

# You can also query them via HTTP
# ```
# In [1]: !curl http://localhost:8000/table/average/1
# {"user": "1", "average": 3.0408163265306123}
# In [2]: !curl http://localhost:8000/table/average
# [{"user": "1", "average": 2.953125}, {"user": "6", "average": 2.75}, {"user": "7", "average": 3.096774193548387}, {"user": "10", "average": 3.207547169811321}, {"user": "2", "average": 3.066666666666667}, {"user": "8", "average": 2.75}, {"user": "9", "average": 2.761904761904762}, {"user": "3", "average": 3.142857142857143}, {"user": "4", "average": 3.0434782608695654}, {"user": "5", "average": 3.1666666666666665}]
# ```

assert False, "Implementation stops here."

# # Advanced: Maintaining user vectors
# Now that we've setup a simple feature table and run some queries, we can create a more realistic feature table: a user vector representing their movie tastes.
#
# In this example, we'll assume we already have pre-computed movie vectors which are held constant. User vectors are updated over time as new rating informatio is recieved.

# In[ ]:

import pandas as pd


class UserVectors(Operator):
    def __init__(self, schema, movie_vectors_file):
        self.user_ratings = {}
        self.movie_vectors = pd.read_csv(movie_vectors_file)

    def on_record(self, rating: Record, movie_vector: Record):
        pass


user_vectors = source.map(UserVectors, args=(source_schema, "movie_vectors.csv"))


# ## Prioritizing Active Users
# Ralf allows for key-level prioritization policies. Say that we want to prioritize computing updates to user vectors for users who especially active. We can use activity data to implement a prioritized lottery scheduling policy.

# In[ ]:


user_activity = pd.read_csv("user_active_time.csv")


# For example, we can set the subsampling rate of the data to be inversely proportional to how active the user is.

# In[ ]:


class SampleActiveUsers(LoadSheddingPolicy):
    def __init__(self, user_activity_csv):
        user_activity = pd.read_csv("user_active_time.csv")
        self.weights = {}

    def process(self, record: Record) -> bool:
        return random.random() < self.weights[record.user]


# Alternatively, we can create a key prioritization policy which prioritizes keys uses lottery scheduling.

# In[ ]:


class LotteryScheduling(PrioritizationPolicy):
    def __init__(self, user_activity_csv):
        user_activity = pd.read_csv("user_active_time.csv")
        self.weights = user_activity.dict()

    def choose(self, keys: List[KeyType]):
        pass


user_vectors.add_prioritization_policy(lottery_scheduling)


# ## Updating features lazily
# We need append tables for this....

# In[ ]:
