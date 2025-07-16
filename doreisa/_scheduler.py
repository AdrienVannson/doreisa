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

    # TODO this will not work all the time
    assert isinstance(keys, list) and len(keys) == 1
    if isinstance(keys[0], list):
        assert len(keys[0]) == 1
        key = keys[0][0]
    else:
        key = keys[0]

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
            actors_dependencies = [explore(dep) for dep in get_dependencies(dsk, k)]

            if not actors_dependencies:
                # The task is a leaf, we use a random actor
                partition[k] = random.randint(0, len(scheduling_actors) - 1)
            else:
                partition[k] = Counter(actors_dependencies).most_common(1)[0][0]

        return partition[k]

    explore(key)

    log("2. Graph partitionning done", debug_logs_path)

    partitionned_graphs: dict[int, dict] = {actor_id: {} for actor_id in range(len(scheduling_actors))}

    for k, v in dsk.items():
        actor_id = partition[k]

        partitionned_graphs[actor_id][k] = v

        for dep in get_dependencies(dsk, k):
            if partition[dep] != actor_id:
                partitionned_graphs[actor_id][dep] = ScheduledByOtherActor(partition[dep])

    log("3. Partitionned graphs created", debug_logs_path)

    graph_id = random.randint(0, 2**128 - 1)

    ray.get(
        [
            actor.schedule_graph.options(enable_task_events=False).remote(graph_id, partitionned_graphs[id])
            for id, actor in enumerate(scheduling_actors)
            if partitionned_graphs[id]
        ]
    )

    log("4. Graph scheduled", debug_logs_path)

    res_ref = scheduling_actors[partition[key]].get_value.remote(graph_id, key)

    if kwargs.get("ray_persist"):
        if isinstance(keys[0], list):
            return [[res_ref]]
        return [res_ref]

    res = ray.get(ray.get(res_ref))

    log("5. End Doreisa scheduler", debug_logs_path)

    if isinstance(keys[0], list):
        return [[res]]
    return [res]
