import asyncio
import ray
import ray.util.dask
import dask
import dask.array as da
import numpy as np
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
import math


def init():
    ray.init()
    ray.util.dask.enable_dask_on_ray()


@dask.delayed
def ray_to_dask(x):
    return x


class DaskArray:
    def __init__(self, nb_chunks_per_dim: tuple[int, ...]) -> None:
        # Number of chunks in each dimension
        self.nb_chunks_per_dim = nb_chunks_per_dim

        self.nb_chunks = math.prod(nb_chunks_per_dim)

        # chunks[t][(x, y, z, ...)] is the chunk (x, y, z, ...) of the array at time t
        self.chunks: list[dict[tuple[int, ...], ray.ObjectRef]] = []

        # To wait until all the data for the current step is available
        self.data_ready = asyncio.Barrier(self.nb_chunks + 1)

        # Tell the MPI workers they can resume the simulation
        self.next_simulation_step_event = asyncio.Event()


@ray.remote
class SimulationHead:
    def __init__(self) -> None:
        # For each name, the corresponding array
        self.arrays: dict[str, DaskArray] = {}

        # This event is called when a new array is created
        self.new_array_event = asyncio.Event()

    def create_array(self, name: str, nb_chunks_per_dim: tuple[int, ...]) -> None:
        if arr := self.arrays.get(name):
            if arr.nb_chunks_per_dim != nb_chunks_per_dim:
                raise ValueError(
                    "two different values of `nb_chunks_per_dim` provided for the same array"
                )
        
        else:
            self.arrays[name] = DaskArray(nb_chunks_per_dim)
            self.new_array_event.set()

    async def add_chunk(self, array_name: str, timestep: int, position: tuple[int, ...], chunk: list[ray.ObjectRef]) -> None:
        # The list for grid prevents ray from dereferencing the object
        chunk = chunk[0]

        array = self.arrays[array_name]
        if len(array.chunks) <= timestep:
            array.chunks.append({})

        # TODO is it too strict for some simulations?
        assert timestep == len(array.chunks) - 1
        array.chunks[timestep][position] = chunk

        # Inform that the data is available
        await array.data_ready.wait()

        # Wait until the data is not needed anymore
        await array.next_simulation_step_event.wait()

    def simulation_step_complete(self) -> None:
        """
        Tell all the waiting MPI workers that the data is not needed anymore.
        """
        for array in self.arrays.values():
            array.next_simulation_step_event.set()

    async def get_full_array(self, name: str) -> np.ndarray | None:
        """
        Return the last timestep of the array. Should be called only once per timestep.
        """
        while name not in self.arrays:
            await self.new_array_event.wait()
        
        array = self.arrays[name]

        # Wait until all the data for the step is available
        await array.data_ready.wait()

        # Make available in dask
        # TODO don't hardcode the size
        chunks = {k: da.from_delayed(ray_to_dask(v), (32, 32), dtype=float) for k, v in array.chunks[-1].items()}

        # Remove the ghost cells
        # TODO don't do it here
        chunks = {k: v[1:-1, 1:-1] for k, v in chunks.items()}

        # TODO only works for 2D
        # Return the complete grid
        return da.block([[chunks[(i, j)] for i in range(array.nb_chunks_per_dim[0])] for j in range(array.nb_chunks_per_dim[1])])


async def start(callback, array_names: list[str], window_size=1) -> None:
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

    print("Waiting to start the simulation...")

    step = 0

    grids = []

    while True:
        # TODO support multiple arrays
        complete_grid = ray.get(head.get_full_array.remote(array_names[0]))
        assert isinstance(complete_grid, da.Array)

        if step == 0:
            print("Simulation started!")

        grids.append(complete_grid)

        if len(grids) > window_size:
            grids = grids[1:]
        if len(grids) == window_size:
            callback(grids, step)

        step += 1

        head.simulation_step_complete.remote()
