import random
import sys
import time

import numpy as np
import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

NB_ACTORS = int(sys.argv[1])
NB_ITERATIONS = int(sys.argv[2])

# Wait until all the nodes have joined the cluster
time.sleep(4)

ray.init()

assert len(ray.nodes()) == 10


@ray.remote
def create_big_array():
    return ray.put(np.random.random((1000, 1000)))


@ray.remote
def identity(x):
    return x


refs = []
for node in ray.nodes():
    refs.append(
        create_big_array.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=node["NodeID"],
                soft=False,
            ),
        ).remote()
    )


@ray.remote
class SchedulingActor:
    def __init__(self, refs: list[ray.ObjectRef]):
        self.refs = refs

    def start_tasks(self):
        futures = []
        for _ in range(NB_ITERATIONS):
            ref = random.choice(self.refs)
            futures.append(identity.remote(ref))

        ray.get(futures)


scheduling_actors = [
    SchedulingActor.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=ray.get_runtime_context().get_node_id(),
            soft=False,
        )
    ).remote(refs)
    for _ in range(NB_ACTORS)
]

begin = time.time()
ray.get([a.start_tasks.remote() for a in scheduling_actors])
end = time.time()

with open("ray-benchmark.txt", "a") as f:
    f.write(f"{NB_ACTORS} {NB_ITERATIONS} {end - begin}\n")
