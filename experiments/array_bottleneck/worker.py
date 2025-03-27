import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from doreisa.simulation_node import Client

import numpy as np

rank = int(os.environ["OMPI_COMM_WORLD_RANK"])
total = int(os.environ["OMPI_COMM_WORLD_SIZE"])

client = Client(rank)

for i in range(400):
    array = np.array([rank + 1000_000_000*i], dtype=np.int64)

    client.add_chunk("array", (rank,), (total,), array, store_externally=False)
