import hashlib
import ray
from ray.actor import ActorHandle
from typing import Any, Dict, List, Type

from ralf.core import RalfContext
from copy import copy


class OperatorActorPool:
    """Contains a set number of Ray Actors."""

    def __init__(self, handles: List[ActorHandle]):
        self.handles = handles

    @classmethod
    def make_replicas(
        cls: Type["OperatorActorPool"],
        num_replicas: int,
        actor_class: Type,
        ralf_context: RalfContext,
        actor_options: Dict[str, Any] = dict(),
        *init_args,
        **init_kwargs
    ):
        assert num_replicas > 0

        handles = []
        for i in range(num_replicas):
            ralf_context = copy(ralf_context)
            ralf_context.current_shard_idx = i

            handle = actor_class.options(**actor_options).remote(
                *init_args, **init_kwargs
            )
            handles.append(handle)

        return cls(handles)

    def hash_key(self, key: str) -> int:
        assert isinstance(key, str)
        hash_val = hashlib.sha1(key.encode("utf-8")).hexdigest()
        return int(hash_val, 16)

    def choose_actor(self, key: str) -> ActorHandle:
        return self.handles[self.hash_key(key) % len(self.handles)]

    def broadcast(self, attr, *args):
        # TODO: fix having to wait
        outputs = []
        for handle in self.handles:
            outputs.append(getattr(handle, attr).remote(*args))
        return outputs
