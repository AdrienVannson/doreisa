import os

os.environ["RAY_worker_register_timeout_seconds"] = "3600"

import sys

import numpy as np
import ray

rank = int(sys.argv[1])
total = int(sys.argv[2])
nb_chunks_sent = int(sys.argv[3])

ray.init(address="auto")

head = ray.get_actor("simulation_head", namespace="doreisa")

# Many arrays will be created on the same machine
# It might hang the simulation if not enough memory is available
arrays = [ray.put(np.random.randint(0, 100, size=(100, 100), dtype=np.int64)) for _ in range(nb_chunks_sent)]

for i in range(260):
    ray.get(head.add_chunk.remote(arrays, total))
