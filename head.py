import asyncio
from matplotlib import pyplot as plt
import doreisa.head_node as doreisa
import dask.array as da
import numpy as np

doreisa.init()


def preprocessing(chunks: tuple[np.array], rank: int, timestep: int) -> dict[tuple[str, tuple[int, ...]], np.array]:
    temperatures = chunks[0]

    return {
        ("temperatures", (rank % 3, rank // 3)): temperatures[1:-1, 1:-1],
    }


def simulation_callback(grids: list[da.Array], step: int):
    if step % 100 == 0:
        print("Simulation step", step)
        grid = grids[1]
        plt.imsave(f"temperatures/{str(step // 100).zfill(3)}.png", grid, cmap="gray")

        diff = grids[1] - grids[0]
        plt.imsave(
            f"derivatives/{str(step // 100).zfill(3)}.png", 5 * diff, cmap="gray"
        )


asyncio.run(doreisa.start(preprocessing, simulation_callback, ["temperatures"], window_size=2))
