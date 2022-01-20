# Concepts 

## FeatureFrame (prev: Table)
Ralf's abstractions are centered around tables. Tables are defined by some schema, parent tables, and an operator to transform the parent tables. Sources are a special type of table which do not need parent tables or an operator, as they represent raw data sources.  

## Source 
Ralf ingests data through sources, which can be either streaming sources (e.g. kafka topics) or static sources (e.g. DB table, CSV file). Sources are a special type of table which don't have parent tables, but can otherwise be treated in the same was as tables by setting them as queryable or synchronizing table values with an external connector. 

### `KafkaSource`
You can ingest streaming data sources using a `KafkaSource`: 
```
table = ralf.from_kafka(topic="topic")
```
The table schema will be auto-generated from the Kafka records.

### `HTTPSource` (todo: add listener?)
TODO

### `CSVSource`
You can ingest static data sources using a `CSVSource`: 
```
table = ralf.from_csv(filename="file.csv")
```
The table schema will be auto-generated from the CSV columns. 

### `PostgresSource`
TODO

### Custom Sources
A custom source can be defined by extending the `Source` object. 
```
class MyCustomSource(Source): 

    def __init__(self, schema: Schema): 
        super.__init__(self, schema) 
        # TODO: initialiation code

    def next(self) -> List[Record]: 
        # TODO: define how to return batch of new records
```
ralf will call the `next()` method to pull new data. 
        
## Transform (prev: Operator)
Operators define how to transform parent tables into new tables. Operators are implemented as Ray Actors (TODO: link), and define transformations for updating table values with parent table updates. 
```
class MyOperator(Operator): 
    
    def __init__(self, schema: Schema): 
        super.__init__(self, schema)
        # TODO: initialization code

    def on_record(self, record: Record): 
        # TODO: transformation code
        return Record(...)
```

ralf supports a number of built-in operators, such as: 

* `.window(slide_size: int, window_size: int)`
* `.groupby(key, value)`

```{eval-rst}
.. autofunction:: ralf.operator.Operator
```

## Connectors 
Feature rows are stored in ralf internal tables, but can also be synced to external state connectors. You can query the state connectors for feature stores directly, rather than using ralf's client API. 

### Redis
TODO

### SQLLite
TODO

### Custom Connectors
TODO


