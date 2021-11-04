import asyncio
import json
from itertools import chain
from typing import Dict, List

import fastapi
import numpy as np
from fastapi import FastAPI
from ray import serve
import ray
from ray.actor import ActorClass

from ralf.operator import ActorPool, Operator
from ralf.operators.join import LeftJoin
from ralf.operators.logging import Print
from ralf.operators.window import SlidingWindow
from ralf.policies.base import LoadSheddingPolicy, PrioritizationPolicy
from ralf.state import Record

_queryable_tables: Dict[str, "Table"] = dict()

# User facing Table API
class Table:
    def __init__(
        self,
        parents: List["Table"],
        operator: Operator,
        *operator_args,
        **operator_kwargs,
    ):
        self.num_replicas = 1
        if "num_replicas" in operator_kwargs:
            self.num_replicas = operator_kwargs["num_replicas"]
            del operator_kwargs["num_replicas"]

        if not isinstance(operator, ActorClass):
            operator = ray.remote(operator)

        # TODO: Add schema info
        self.operator = operator
        self.args = operator_args
        self.kwargs = operator_kwargs
        self.pool = ActorPool.make_replicas(
            self.num_replicas, operator, *operator_args, **operator_kwargs
        )
        self.pool.broadcast("set_parents", [parent.pool for parent in parents])
        self.parents = parents
        self.children = []
        self._is_source = len(parents) == 0
        self.is_queryable = False

    def add_load_shedding(self, policy_class, *args, **kwargs):
        assert issubclass(policy_class, LoadSheddingPolicy)
        self.pool.broadcast("set_load_shedding", policy_class, *args, **kwargs)
        return self

    def add_prioritization_policy(self, policy_class, *args, **kwargs):
        assert issubclass(policy_class, PrioritizationPolicy)
        self.pool.broadcast(
            "set_intra_key_prioritization", policy_class, *args, **kwargs
        )
        return self

    def __repr__(self) -> str:
        return f"Table({self.operator.__ray_metadata__.class_name})"

    def debug_state(self):
        return {
            "operator_name": repr(self),
            "parents": [repr(parent_table) for parent_table in self.parents],
            "children": [repr(child_table) for child_table in self.children],
            "is_source": self.is_source(),
            "operator_args": [str(arg) for arg in self.args],
            "operator_kwargs": {k: str(v) for k, v in self.kwargs.items()},
            "actors_state_ref": self.pool.broadcast("debug_state"),
            "actor_pool_size": len(self.pool.handles),
        }

    def _add_child(self, table: "Table"):
        self.children.append(table)
        # update actor pool
        self.pool.broadcast("set_children", [child.pool for child in self.children])

    def is_source(self):
        return self._is_source

    def map(self, operator: Operator, *operator_args, **operator_kwargs):
        if operator_kwargs.get("args"):
            operator_args = operator_kwargs.pop("args")
        child_table = Table([self], operator, *operator_args, **operator_kwargs)
        self._add_child(child_table)
        return child_table

    def join(
        self,
        right_table: "Table",
        operator: LeftJoin,
        *operator_args,
        **operator_kwargs,
    ):
        # TODO: FIX - get actor handle (pass schemas)
        child_table = Table(
            [self, right_table], operator, *operator_args, **operator_kwargs
        )
        self._add_child(child_table)
        right_table._add_child(child_table)
        return child_table

    # def window(self, window_size, num_replicas=1):
    #     child_table = Table(
    #         [self], TumblingWindow, window_size, "key", str, num_replicas=num_replicas
    #     )
    #     self._add_child(child_table)
    #     return child_table

    def window(
        self,
        window_size,
        slide_size,
        num_replicas=1,
        num_worker_threads=1,
        per_key_slide_size_plan_file=None,
    ):
        child_table = Table(
            [self],
            SlidingWindow,
            window_size,
            slide_size,
            "key",
            str,
            num_replicas=num_replicas,
            num_worker_threads=num_worker_threads,
            per_key_slide_size_plan_file=per_key_slide_size_plan_file,
        )
        self._add_child(child_table)
        return child_table

    def print(self):
        child_table = Table([self], Print, "key", str)
        self._add_child(child_table)
        return child_table

    # TODO: remove?
    def get(self, key: str):
        return self.pool.get(key)

    async def get_async(self, key):
        return await self.pool.get_async(key)

    async def get_all_async(self):
        return await asyncio.gather(*self.pool.get_all_async())

    def as_queryable(self, table_name):
        _queryable_tables[table_name] = self
        self.is_queryable = True
        return self


def deploy_queryable_server():
    # NOTE(simon): this encoder is able to handle arbitrary nesting of Record and make sure it's JSON serializable.
    class RalfEncoder(json.encoder.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, Record):
                return obj.entries
            elif isinstance(obj, np.ndarray):
                return obj.tolist()

    app = FastAPI()

    @serve.deployment(route_prefix="/")
    @serve.ingress(app)
    class QueryableServer:
        def __init__(self) -> None:
            # TODO(simon):
            # This _queryable_tables variable will be serialized whenever QueryableServer.deploy() is called.
            # Of course we can update it dynamically as well in the future.
            self._queryable_tables = _queryable_tables

        @app.get("/table/{table_name}/{key}")
        async def point_query(self, table_name: str, key: str):
            if table_name not in self._queryable_tables:
                return fastapi.responses.JSONResponse(
                    {
                        "error": f"{table_name} not found, existing tables are {list(self._queryable_tables.keys())}"
                    },
                    status_code=404,
                )
            resp = await self._queryable_tables[table_name].get_async(key)
            return fastapi.responses.Response(
                json.dumps(resp.entries, cls=RalfEncoder), media_type="application/json"
            )

        @app.get("/table/{table_name:str}")
        async def bulk_query(self, table_name: str):
            if table_name not in self._queryable_tables:
                return fastapi.responses.Response(
                    f"{table_name} not found, existing tables are {list(self._queryable_tables.keys())}",
                    status_code=404,
                )
            resp = await self._queryable_tables[table_name].get_all_async()
            resp = [
                record.entries for record in chain.from_iterable(resp)
            ]  # need to flatten the list
            return fastapi.responses.Response(
                json.dumps(resp, cls=RalfEncoder), media_type="application/json"
            )

        @app.get("/range_query/{table_name:str}")
        def range_query(self, table_name: str, start, end):
            return fastapi.responses.Response("not implemented", status_code=501)

    serve.start()
    QueryableServer.deploy()
