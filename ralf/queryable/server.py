import json
from ralf.state import Record
import numpy as np
from fastapi import FastAPI
from ray import serve
import fastapi.responses
from itertools import chain


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
        def __init__(self, _queryable_tables) -> None:
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
