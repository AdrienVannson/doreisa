import asyncio
from matplotlib import pyplot as plt
import doreisa
import dask.array as da

doreisa.init()


def simulation_callback(grids: list[da.Array], step: int):
    if step % 100 == 0:
        print("Simulation step", step)
        grid = grids[1]
        plt.imsave(f"temperatures/{str(step // 100).zfill(3)}.png", grid, cmap="gray")

        diff = grids[1] - grids[0]
        plt.imsave(
            f"derivatives/{str(step // 100).zfill(3)}.png", 5 * diff, cmap="gray"
        )


asyncio.run(doreisa.start(simulation_callback, window_size=2))
