import asyncio
import ray
import ray.util.dask
import dask
import dask.array as da
import numpy as np
import time
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


def init():
    ray.init()
    ray.util.dask.enable_dask_on_ray()


@dask.delayed
def ray_to_dask(x):
    return x


@ray.remote
class SimulationHead:
    def __init__(self) -> None:
        self.simulation_data: dict[int, list[ray.ObjectRef]] = {}

        # To wait until all the data for the current step is available
        self.data_ready = asyncio.Barrier(9 + 1)

        # Tell the MPI workers they can resume the simulation
        self.next_simulation_step_event = asyncio.Event()

    def nb_workers_ready(self) -> int:
        return len(self.simulation_data)

    def set_worker_ready(self, worker_id: int) -> None:
        self.simulation_data[int(worker_id)] = []

    async def simulation_step(self, worker_id: int, grid: list[ray.ObjectRef]) -> int:
        # The list for grid prevents ray from dereferencing the object

        self.simulation_data[int(worker_id)].append(grid[0])

        # Inform that the data is available
        await self.data_ready.wait()

        # Wait until the data is not needed anymore
        await self.next_simulation_step_event.wait()

    def simulation_step_complete(self) -> None:
        """
        Tell all the waiting MPI workers that the data is not needed anymore.
        """
        self.next_simulation_step_event.set()

    async def complete_grid(self, step: int) -> np.ndarray | None:
        # Wait until all the data for the step is available
        await self.data_ready.wait()

        grids = [self.simulation_data[i][step] for i in range(9)]

        # Make available in dask
        grids = [da.from_delayed(ray_to_dask(g), (32, 32), dtype=float) for g in grids]

        # Remove the ghost cells
        grids = [g[1:-1, 1:-1] for g in grids]

        # Return the complete grid
        return da.block([[grids[3 * l + c] for c in range(3)] for l in range(3)])


async def start(callback, window_size=1) -> None:
    # The workers will be able to access to this actor using its name
    head = SimulationHead.options(
        name="simulation_head",
        namespace="doreisa",
        # Schedule the actore on this node
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=ray.get_runtime_context().get_node_id(),
            soft=False,
        ),
    ).remote()

    print("Waiting for the workers to join the cluster...")

    while ray.get(head.nb_workers_ready.remote()) < 9:
        time.sleep(0.1)

    print("All workers have joined! Simulation ongoing...")

    step = 0

    grids = []

    while True:
        complete_grid = ray.get(head.complete_grid.remote(step))
        assert isinstance(complete_grid, da.Array)

        grids.append(complete_grid)

        if len(grids) > window_size:
            grids = grids[1:]
        if len(grids) == window_size:
            callback(grids, step)

        step += 1

        head.simulation_step_complete.remote()
