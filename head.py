import asyncio
from matplotlib import pyplot as plt
import doreisa.head_node as doreisa
import dask.array as da
import numpy as np

doreisa.init()


def preprocess_temperatures(temperatures: np.array) -> np.array:
    """
    Remove the ghost cells from the array.
    """
    return temperatures[1:-1, 1:-1]


def simulation_callback(temperatures: list[da.Array], timestep: int):
    if timestep % 100 == 0 and timestep:
        print("Simulation step", timestep)
        # temp = temperatures[1]
        temp = temperatures
        plt.imsave(f"temperatures/{str(timestep // 100).zfill(3)}.png", temp, cmap="gray")

        # diff = temperatures[1] - temperatures[0]
        # plt.imsave(
        #     f"derivatives/{str(timestep // 100).zfill(3)}.png", 5 * diff, cmap="gray"
        # )


asyncio.run(doreisa.start(simulation_callback, [
    doreisa.DaskArrayInfo("temperatures", (3, 3), window_size=2, preprocess=preprocess_temperatures),
]))
