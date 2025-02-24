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
class SimulationHead():
    def __init__(self) -> None:
        self.simulation_data: dict[int, list[ray.ObjectRef]] = {}

    def nb_workers_ready(self) -> int:
        return len(self.simulation_data)

    def set_worker_ready(self, worker_id: int) -> None:
        self.simulation_data[int(worker_id)] = []

    def simulation_step(self, worker_id: int, grid: list[ray.ObjectRef]) -> int:
        # The list for grid prevents ray from dereferencing the object

        self.simulation_data[int(worker_id)].append(grid[0])

    def complete_grid(self, step: int) -> np.ndarray | None:
        grids = []
        for i in range(9):
            try:
                grids.append(self.simulation_data[i][step])
            except IndexError:
                return None

        # Make available in dask
        grids = [da.from_delayed(ray_to_dask(g), (12, 12), dtype=float) for g in grids]

        # Remove the ghost cells
        grids = [g[1:-1, 1:-1] for g in grids]

        # Return the complete grid
        return da.block([
            [grids[3*l+c] for c in range(3)]
            for l in range(3)
        ])


def start(callback) -> None:
    # The workers will be able to access to this actor using its name
    head = SimulationHead.options(
        name="simulation_head",
        namespace="doreisa",

        # Schedule the actore on this node
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=ray.get_runtime_context().get_node_id(),
            soft=False,
        )
    ).remote()

    print("Waiting for the workers to join the cluster...")

    while ray.get(head.nb_workers_ready.remote()) < 9:
        time.sleep(0.1)

    print("All workers have joined! Simulation ongoing...")

    step = 0

    while step < 1000:
        complete_grid = ray.get(head.complete_grid.remote(step))
        while complete_grid is None:
            time.sleep(0.1)
            complete_grid = ray.get(head.complete_grid.remote(step))

        assert isinstance(complete_grid, da.Array)

        callback(complete_grid, step)
        step += 1
