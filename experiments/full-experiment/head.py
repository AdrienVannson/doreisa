import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

import asyncio
import doreisa.head_node as doreisa
import dask.array as da
import numpy as np
import time

doreisa.init()

start_time = None


def simulation_callback(arrays: list[da.Array], timestep: int):
    arr = arrays[0]

    dsk = arr.mean()
    dsk.compute()

    if timestep == 20:
        global start_time
        start_time = time.time()

    if timestep == 120:
        end_time = time.time()

        with open("results.txt", "a") as f:
            f.write(f"{np.prod(arr.numblocks)} {1000 * (end_time - start_time) / 100}\n")

asyncio.run(doreisa.start(simulation_callback, [
    doreisa.DaskArrayInfo("arrays", window_size=1),
]))
