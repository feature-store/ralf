import json
import os
import time
from collections import deque
from itertools import chain
from pprint import pformat
from typing import Optional, Set

import ray

from ralf.table import Table, deploy_queryable_server


class Ralf:
    def __init__(
        self,
        metric_dir: Optional[str] = None,
        log_wandb: Optional[bool] = False,
        exp_id: Optional[str] = None,
    ):
        if not ray.is_initialized():
            ray.init()
        self.tables = {}

        self.metric_dir = self._make_metric_dir(metric_dir)
        self.metric_file = open(os.path.join(self.metric_dir, "snapshots.jsonl"), "w")

        self.log_wandb = log_wandb
        if self.log_wandb:
            import wandb
            wandb.init(project="stl", entity="ucb-ralf", group=exp_id)
            wandb.run.name = exp_id

    def _make_metric_dir(self, metric_dir: Optional[str] = None):
        if metric_dir is None:
            metric_dir = f"/tmp/ralf/{int(time.time())}_metrics"
            os.makedirs(metric_dir, exist_ok=True)
        else:
            assert os.path.isdir(metric_dir)

        # LATEST_PATH = "/tmp/ralf/latest"
        # if os.path.exists(LATEST_PATH):
        #     os.remove(LATEST_PATH)
        # os.makedirs("/tmp/ralf", exist_ok=True)
        # os.symlink(metric_dir, LATEST_PATH, target_is_directory=True)

        print(f"Storing operators metrics at {metric_dir}")
        return metric_dir

    def _visit_all_tables(self) -> Set[Table]:
        """Returns all tables registered with Ralf.
        Sometimes Ralf adds operators to user's graph, like mapper, window, that is not
        visible inside this instance.
        """
        visit_queue = deque(list(self.tables.values()))
        visited = set()
        while len(visit_queue) != 0:
            table = visit_queue.pop()
            visited.add(table)
            for next_table in chain(table.parents, table.children):
                if next_table not in visited:
                    visit_queue.appendleft(next_table)
        return visited

    def pipeline_view(self):
        view = {repr(table): table.debug_state() for table in self._visit_all_tables()}
        # actor_state is fetch in parallel, let's block and get all of them here.
        states = ray.get(
            list(
                chain.from_iterable(
                    [value["actors_state_ref"] for value in view.values()]
                )
            )
        )
        for value in view.values():
            pool_size = value["actor_pool_size"]
            state, states = states[:pool_size], states[pool_size:]
            value["actor_state"] = state
            del value["actors_state_ref"]
        return view

    def snapshot(self):
        "Perform a snapshot of the system state and write to disk."
        snapshot_start = time.time()
        data = self.pipeline_view()
        snapshot_duration = time.time() - snapshot_start
        serialized = json.dumps(
            dict(
                snapshot_start=snapshot_start,
                snapshot_duration=snapshot_duration,
                data=data,
            )
        )

        if self.log_wandb:
            wandb.log({"snapshot_duration": snapshot_duration})
            wandb.log({"raw_json": wandb.Html(serialized)})
            for actor_name in data.keys():
                wandb.log(
                    {
                        f"threadpool_size/{actor_name}": wandb.Histogram(
                            [
                                data[actor_name]["actor_state"][index][
                                    "thread_pool_size"
                                ]
                                for index in range(data[actor_name]["actor_pool_size"])
                            ]
                        )
                    }
                )
                wandb.log(
                    {
                        f"queue_size/{actor_name}": wandb.Histogram(
                            [
                                data[actor_name]["actor_state"][index]["queue_size"]
                                for index in range(data[actor_name]["actor_pool_size"])
                            ]
                        )
                    }
                )
                wandb.log(
                    {
                        f"num_records/{actor_name}": wandb.Histogram(
                            [
                                data[actor_name]["actor_state"][index]["table"][
                                    "num_records"
                                ]
                                for index in range(data[actor_name]["actor_pool_size"])
                            ]
                        )
                    }
                )
                wandb.log(
                    {
                        f"num_updates/{actor_name}": wandb.Histogram(
                            [
                                data[actor_name]["actor_state"][index]["table"][
                                    "num_updates"
                                ]
                                for index in range(data[actor_name]["actor_pool_size"])
                            ]
                        )
                    }
                )
                wandb.log(
                    {
                        f"memory/{actor_name}": wandb.Histogram(
                            [
                                data[actor_name]["actor_state"][index]["process"][
                                    "memory_mb"
                                ]
                                for index in range(data[actor_name]["actor_pool_size"])
                            ]
                        )
                    }
                )
                wandb.log(
                    {
                        f"cpu/{actor_name}": wandb.Histogram(
                            [
                                data[actor_name]["actor_state"][index]["process"][
                                    "cpu_percent"
                                ]
                                for index in range(data[actor_name]["actor_pool_size"])
                            ]
                        )
                    }
                )

        self.metric_file.write(serialized)
        self.metric_file.write("\n")
        self.metric_file.flush()
        return snapshot_duration

    def run(self):
        print(pformat(self.pipeline_view()))

        if any(table.is_queryable for table in self._visit_all_tables()):
            deploy_queryable_server()

        # pull data from sources
        for table in self.tables.values():
            if table.is_source():
                table.pool.broadcast("_next")

    def deploy(self, table: Table, name: str):
        # TODO: only execute tables/ops which are deployed
        self.tables[name] = table

    def get_table(self, name: str) -> Table:
        return self.tables[name]

    def create_source(self, operator_class, args=None):
        table = Table([], operator_class, *args)
        self.deploy(table, "source")
        return table
