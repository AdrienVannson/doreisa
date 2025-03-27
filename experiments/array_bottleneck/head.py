import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import asyncio
import doreisa.head_node as doreisa
import dask.array as da
import time

doreisa.init()

nb_nodes = int(sys.argv[1])

start_time = None

def simulation_callback(array: list[da.Array], timestep: int):
    if timestep == 50:  # Allow some warmup time
        global start_time
        start_time = time.time()

    if timestep == 350:
        assert start_time is not None
        end_time = time.time()
        print(f"Time taken: {end_time - start_time}")

        with open("experiments/array_bottleneck/measurements.txt", "a") as f:
            f.write(f"{nb_nodes} {array[0].shape[0]} {end_time - start_time}\n")

    if timestep % 50 == 0:
        print(f"Time step {timestep}")

asyncio.run(doreisa.start(simulation_callback, [doreisa.DaskArrayInfo("array", window_size=1)]))
