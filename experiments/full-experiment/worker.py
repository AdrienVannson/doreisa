import os

os.environ["RAY_worker_register_timeout_seconds"] = "3600"

import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

import sys
import time

import numpy as np

from doreisa.simulation_node import Client

rank, total = int(sys.argv[1]), int(sys.argv[2])

client = Client(rank)
array = np.random.randint(0, 100, size=(1024, 1024), dtype=np.int64)

for _ in range(125):
    client.add_chunk("arrays", (rank,), (total,), array, store_externally=False)

time.sleep(30)
