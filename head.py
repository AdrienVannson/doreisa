import asyncio
from matplotlib import pyplot as plt
import doreisa
import dask.array as da

doreisa.init()

def simulation_callback(grid: da.Array, step: int):
    if step % 30 == 0:
        plt.imsave(f"output/{str(step // 30).zfill(3)}.png", grid, cmap='gray')

asyncio.run(doreisa.start(simulation_callback))
