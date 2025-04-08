import asyncio
from matplotlib import pyplot as plt
import doreisa.head_node as doreisa
import dask.array as da
import numpy as np
import os

doreisa.init()


def preprocess_temperatures(temperatures: np.ndarray) -> np.ndarray:
    """
    Remove the ghost cells from the array.
    """
    return temperatures[1:-1, 1:-1]


def simulation_callback(temperatures: list[da.Array], timestep: int):
    print("Simulation step", timestep)
    if timestep % 10 == 9:
        temp1 = temperatures[1][10:-10:50, 10:-10:50]
        temp0 = temperatures[0][10:-10:50, 10:-10:50]
        os.makedirs("temperatures", exist_ok=True)
        plt.imsave(f"temperatures/{str(timestep // 10).zfill(3)}.png", temp1, cmap="gray")

        diff = temp1 - temp0
        a, b = diff.min().compute(), diff.max().compute()
        os.makedirs("derivatives", exist_ok=True)
        plt.imsave(f"derivatives/{str(timestep // 10).zfill(3)}.png", ((diff - a) / (b - a)).compute(), cmap="gray")


asyncio.run(
    doreisa.start(
        simulation_callback,
        [
            doreisa.DaskArrayInfo("temperatures", window_size=2, preprocess=preprocess_temperatures),
        ],
    )
)
