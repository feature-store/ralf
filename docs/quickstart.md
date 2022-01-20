# Quickstart 

To setup a simple example, we will a stream of HackerNews comments on articles to create feature tables which store the average comment sentiment for each article on HackerNews. 

(create-server)=
## Creating a `ralf` server

To generate data, run the `examples/stream_hn_comments.sh` script inside the examples folder. 
First, we connect create a `Source` 
```
comments = ralf.from_kafka_source(topic="hn_comments")
```
Although the `comments` table is coming from a stream of data, we can treat it like a dataframe object that can be transformed into other tables. In order to calculate the sentiment of each comment, we can create an `Operator` object which loads and runs a sentiment model. 

```
from ralf import Operator

class CommentSentiment(Operator):
    def __init__(self, schema):
        super().__init__(schema)
        self.model = pipeline("sentiment-analysis")

    def on_record(self, record: Record):
        return Record(
            title=record.title,
            comment_id=record.comment_id, 
            sentiment=self.model(record.text)
        )

comments = ralf.from_kafka_source(topic="hn_comments")
sentiment = comments.map(CommentSentiment, args=(sentiment_schema))
``` 
Finally, to create a table tracking the average comment sentiment for each article, we can use built in `group_by
```
class GroupByAverage(Operator): 
    def __init__(self, key: str, value: str, schema): 
        self.state = defaultdict(list)
        self.key = key
        self.value = value

    def on_record(self, record: Record):    
        key = record.get(self.key)
        self.state[key].append(record.get(self.value))
        return Record(key=key, value=np.array(self.state[key]).avg())

user_sentiment = sentiment.map(GroupByAverage, args=("user", "sentiment", user_sentiment_schema))
article_sentiment = sentiment.map(GroupByAverage, args=("title", "sentiment", title_sentiment_schema))
```
To make our tables externally queryable, we can define a handle for each table to be queryable by: 
```
user_sentiment = sentiment
        .map(GroupByAverage, args=("user", "sentiment", user_sentiment_schema))
        .as_queryable("user_sentiment")
article_sentiment = sentiment
        .map(GroupByAverage, args=("title", "sentiment", title_sentiment_schema))
        .as_queryable("article_sentiment")
```
Finally, to run the server, we add: 
```
ralf.run()
```

(create-client)=
## Creating a `ralf` client
Ralf tables set as queryable with the `.as_queryable()` method can be queried by the client. We can create a Ralf client in a seperate script with: 
```python
from ralf import RalfClient

client = RalfClient()
```
The client can be queried by the primary key (set in the table schema) with the `point_query()` method:
```python
user_sentiment = client.point_query(table="user_sentiment", key="bob")
```
You can also query all values in the table with a `bulk_query()`, which returns an iterator returning `batch_size` rows at a time: 
```python
for rows in client.bulk_query(table="user_sentiment", batch_size=100): 
    for user_sentiment in rows: 
        print(user_sentiment)
```
 

```{admonition} TODO 
:class: warning

Change to HTTPSource

```
