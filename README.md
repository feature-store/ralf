# ralf
Ralf is a tool of storing and maintaining feature tables over changing data. 


## Installation
`ralf` can be installed from source via `pip install -e .`. You can then run `import ralf`.

## Quickstart 

```
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

ralf = Ralf()
source = ralf.create_source(FakeSource, args=(["sam", "bob", "sarah"]))
window = source.window(window_size=100, slide_size=50)
average = window.map(Average).as_queryable("average")
```


## Concepts
Ralf's API is centered around `Table` objects which can be treated like database tables or dataframes. `Tables` are defined in terms of one of more parents tables and an `Operator`, which defines how to process new parent data to update the table's values. 

### Sources 
Sources feed raw data into ralf. 

### Operators 

### Processing Policies 

