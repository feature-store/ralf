# ralf
Ralf is a feature store for rapidly changing data. Ralf incrementally propagates raw data changes to derived *feature tables* which are queryable by downstream applications such as model training and inference. 

## Installation
`ralf` can be installed from source via `pip install -e .`. You can then run `import ralf`.

## Quickstart 
Ralf ingests data from user-defined `Source` objects, which can connect streaming services to recieve new data or generate data synthetically. Source data is transformed by operators to define downstream feature tables. In the example below, we create a simple feature pipeline which generates random numbers for a set of keys `["sam", "bob", "sarah"]`, and calculates the average value for each key. 

```python
import ralf 
from ralf import Operator, Source

import time
import random

# define a custom source with generates synthetic data (random numbers for each key) 
@ray.remote 
class FakeSource(Source): 
  def __init__(self, keys): 
    self.keys = keys 
    self.sleep_time = 0.01

  def next(self): 
    records = [Record(key=key, value=random.random()) for key in self.keys]
    time.sleep(self.sleep_time)
    return records

@ray.remote 
class Average(Operator):
  def on_record(self, record: Record) -> Record: 
    return sum(record.values) / len(record.values)

ralf_server = Ralf()
source = ralf_server.create_source(FakeSource, args=(["sam", "bob", "sarah"]))
window = source.window(window_size=100, slide_size=50)
average = window.map(Average).as_queryable("average")
ralf_server.deploy()
```
Once we've deployed the server, we can query feature tables from a `RalfClient`. 
```python
ralf_client = RalfClient() 
res = ralf_client.point_query(table="average", key="bob") # query a single key 
res = ralf_client.bulk_query(table="average") # query all keys 
```

## Concepts
Ralf's API is centered around `Table` objects which can be treated like database tables or dataframes. `Tables` are defined in terms of one of more parents tables and an `Operator`, which defines how to process new parent data to update the table's values. 

### Sources 
Sources feed raw data into ralf. Sources can be from streams (e.g. kafka), files (e.g. CSV files), or custom sources (e.g. synthetically generated data). 
```python 
ralf_server = Ralf()

# Read data from kafka stream 
kafka_source = ralf_server.create_kafka_source(topic="kafka_topic") 

# Read data from CSV file 
csv_source = ralf_server.create_csv_source(filename="csv_filename")

# Ingest data from a custom source 
custom_source = ralf_server.create_source(SourceOperator, args=(...))
```

### Operators 
Opeartors are the featurization transformations for deriving features. Operators define how parent table data should be transformed into child table data. For example, we can define a feature table containing the results of running source data through a model: 
```python
class ModelOperator(Operator): 

  def __init__(self, model_file):
    self.model = load(model_file)
    
  def on_record(self, record: Record): 
    # featurization logic 
    feature = self.model(record.value)
    return Record(key, feature) 
    
feature_table = source.map(ModelOperator, args=("model.pt"))
    
```

### Processing Policies 
Ralf enables customizeable load shedding and prioritization policies at the per-table level. 