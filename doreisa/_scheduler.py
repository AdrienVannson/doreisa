import random
from collections import Counter

import ray
from dask.core import get_dependencies

from doreisa._scheduling_actor import ChunkRef


def doreisa_get(dsk, keys, **kwargs):
    # The graph may be a HighLevelGraph
    dsk = dict(dsk)

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
    scheduling = {k: -1 for k in dsk.keys()}

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
            scheduling[k] = val.actor_id
        else:
            res = [explore(dep) for dep in get_dependencies(dsk, k)]
            scheduling[k] = Counter(res).most_common(1)[0][0]

        return scheduling[k]

    explore(key)

    # Pass the scheduling to the scheduling actors
    dsk_ref, scheduling_ref = ray.put(dsk), ray.put(scheduling)  # noqa: F841

    graph_id = random.randint(0, 2**128 - 1)

    ray.get(
        [
            scheduling_actors[i]
            .schedule_graph.options(enable_task_events=False)
            .remote(dsk_ref, graph_id, scheduling_ref)
            for i in range(len(scheduling_actors))
        ]
    )

    res = scheduling_actors[scheduling[key]].get_value.remote(graph_id, key)

    return [[ray.get(ray.get(res))]]
