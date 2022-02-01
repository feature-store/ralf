input -> pre-process (drop, ack) -> queue -> work -> post-process (metrics, storage, call next)

```python
import ralf

pipeline = ralf.init(metrics_dir=...)

# Build the computation graph
source: FeatureFrame = pipeline.from_source(KafkaSource, "")
mapped: FeatureFrame = source.map(lambda record: record["value"]+1, label="map_func")
custom_transform: FeatureFrame = mapped.transform(MyTransformClass, ...)
joined: FeatureFrame = mapped.join(custom_transform, on="...")
sink: FeatureFrame = joined.window(...).map(SinkOperator)

# Apply policies
processing_policy.apply(pipeline, match_label="map_func")
back_pressure_policy.apply(pipeline)
lottery_scheduling.apply(pipeline, match_label="window")

pipeline.run()
```

Policies Hook:

- Admission control
- Intra-key score
- Inter-key score
- Load shedding
