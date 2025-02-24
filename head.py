import asyncio
from matplotlib import pyplot as plt
import doreisa
import dask.array as da

doreisa.init()

def simulation_callback(grid: da.Array, step: int):
    plt.imsave(f"output/{str(step).zfill(3)}.png", grid, cmap='gray')

asyncio.run(doreisa.start(simulation_callback))
