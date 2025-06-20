import random
import time
from collections import Counter

import ray
from dask.core import get_dependencies

from doreisa._scheduling_actor import ChunkRef, ScheduledByOtherActor


def doreisa_get(dsk, keys, **kwargs):
    debug_logs_path: str | None = kwargs.get("doreisa_debug_logs", None)

    def log(message: str, debug_logs_path: str | None) -> None:
        if debug_logs_path is not None:
            with open(debug_logs_path, "a") as f:
                f.write(f"{time.time()} {message}\n")

    log("1. Begin Doreisa scheduler", debug_logs_path)

    # Sort the graph by keys to make scheduling deterministic
    dsk = {k: v for k, v in sorted(dsk.items())}

    head_node = ray.get_actor("simulation_head", namespace="doreisa")  # noqa: F841

    assert isinstance(keys, list) and len(keys) == 1
    assert isinstance(keys[0], list) and len(keys[0]) == 1
    key = keys[0][0]

    # Find the scheduling actors
    scheduling_actors = ray.get(head_node.list_scheduling_actors.remote())

    # Find a not too bad scheduling strategy
    # Good scheduling in a tree
    partition = {k: -1 for k in dsk.keys()}

    # def explore(key, v: int):
    #     # Only works for trees for now
    #     assert scheduling[key] == -1
    #     scheduling[key] = v
    #     for dep in get_dependencies(dsk, key):
    #         explore(dep, v)

    # scheduling[key] = 0
    # c = 0
    # for dep1 in get_dependencies(dsk, key):
    #     scheduling[dep1] = 0

    #     for dep2 in get_dependencies(dsk, dep1):
    #         scheduling[dep2] = 0

    #         for dep3 in get_dependencies(dsk, dep2):
    #             scheduling[dep3] = 0

    #             for dep4 in get_dependencies(dsk, dep3):
    #                 scheduling[dep4] = 0

    #                 for dep5 in get_dependencies(dsk, dep4):
    #                     explore(dep5, c % len(scheduling_actors))
    #                     c += 1

    # assert -1 not in scheduling.values()

    # scheduling = {k: randint(0, len(scheduling_actors) - 1) for k in dsk.keys()}
    # scheduling = {k: i % len(scheduling_actors) for i, k in enumerate(dsk.keys())}

    # Make sure the leafs are scheduled on the right actor
    # for key, val in dsk.items():
    #     match val:
    #         case ("doreisa_chunk", actor_id):
    #             scheduling[key] = actor_id
    #         case _:
    #             pass

    def explore(k) -> int:
        val = dsk[k]

        if isinstance(val, ChunkRef):
            partition[k] = val.actor_id
        else:
            res = [explore(dep) for dep in get_dependencies(dsk, k)]
            partition[k] = Counter(res).most_common(1)[0][0]

        return partition[k]

    explore(key)

    log("2. Graph partitionning done", debug_logs_path)

    partitionned_graphs: dict[int, dict] = {}

    for k, v in dsk.items():
        actor_id = partition[k]

        if actor_id not in partitionned_graphs:
            partitionned_graphs[actor_id] = {}

        partitionned_graphs[actor_id][k] = v

        for dep in get_dependencies(dsk, k):
            if partition[dep] != actor_id:
                partitionned_graphs[actor_id][dep] = ScheduledByOtherActor(partition[dep])

    log("3. Partitionned graphs created", debug_logs_path)

    graph_id = random.randint(0, 2**128 - 1)

    ray.get(
        [
            actor.store_graph.options(enable_task_events=False).remote(graph_id, partitionned_graphs[id])
            for id, actor in enumerate(scheduling_actors)
        ]
    )

    log("4. Partitionned graphs sent", debug_logs_path)

    ray.get(
        [
            scheduling_actors[i].schedule_graph.options(enable_task_events=False).remote(graph_id)
            for i in range(len(scheduling_actors))
        ]
    )

    log("5. Graph scheduled", debug_logs_path)

    res = ray.get(ray.get(scheduling_actors[partition[key]].get_value.remote(graph_id, key)))

    log("6. End Doreisa scheduler", debug_logs_path)

    return [[res]]
