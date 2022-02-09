## Ralf V2 Architecture

The V2 architecture of consists several couple pieces that's logically separated.

The following pieces are visible to users.

- Transform: implements `def on_event(record: Record) -> None | Record | Iterable[Record]` for feature transformation.
- Scheduler: implements custom scheduling policy/policies to prioritizing records.
- FeatureFrame: represent a an intermediate table in the dataflow, wraps transform and scheduler. (and in the future, storage tables). it is aware of its children.
- RalfApplication: encapsulate a DAG of feature frames and deployment configuration.

The following pieces are internal to implementation.

- Operator: physical manifestation of feature frame. it can be local, ray actor pool, or simulation node.
- Manager: responsible for managing and deploying the operators. called by RalfApplication.
