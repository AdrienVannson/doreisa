import asyncio
import dask.array
import ray
import ray.util.dask
import dask
import dask.array as da
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
import math
from typing import Callable
from dataclasses import dataclass
from typing import Any


def init():
    ray.init()
    ray.util.dask.enable_dask_on_ray()


@dataclass
class DaskArrayInfo:
    """
    Description of a Dask array given by the user.
    """
    name: str
    nb_chunks_per_dim: tuple[int, ...]
    window_size: int = 1
    preprocess: Callable = lambda x: x


class _DaskArrayData:
    """
    All the information concerning the Dask array needed during the simulation (chunks,
    synchronization, etc.).
    """

    def __init__(self, description: DaskArrayInfo) -> None:
        self.description = description
        self.nb_chunks = math.prod(description.nb_chunks_per_dim)

        # chunks[t][(x, y, z, ...)] is the chunk (x, y, z, ...) of the array at time t
        self.chunks: list[dict[tuple[int, ...], da.Array]] = []

        # To wait until all the data for the current step is available
        self.data_ready = asyncio.Barrier(self.nb_chunks + 1)

        # Tell the MPI workers they can resume the simulation
        self.next_simulation_step_event = asyncio.Event()

    async def get_full_array(self) -> da.Array:
        """
        Return the full array for the current timestep.
        """
        # Wait until all the data for the step is available
        await self.data_ready.wait()

        # TODO only works for 2D
        # Return the complete grid
        return da.block([[self.chunks[-1][(i, j)] for i in range(self.description.nb_chunks_per_dim[0])] for j in range(self.description.nb_chunks_per_dim[1])])


@dask.delayed
def ray_to_dask(x):
    return x


@ray.remote
class SimulationHead:
    def __init__(self, arrays_description: list[DaskArrayInfo]) -> None:
        # For each name, the corresponding array
        self.arrays: dict[str, _DaskArrayData] = {
            description.name: _DaskArrayData(description) for description in arrays_description
        }

    def preprocessing_callbacks(self) -> dict[str, Callable]:
        """
        Return the preprocessing callbacks for each array.
        """
        return {name: array.description.preprocess for name, array in self.arrays.items()}

    async def add_chunk(self, array_name: str, timestep: int, position: tuple[int, ...], chunk_ray: list[ray.ObjectRef], chunk_size: tuple[int, ...]) -> None:
        # Putting the chunk in a list prevents ray from dereferencing the object.

        # Convert to a dask array
        chunk = da.from_delayed(ray_to_dask(chunk_ray[0]), chunk_size, dtype=float)

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

    async def get_all_arrays(self) -> dict[str, da.Array]:
        """
        Return all the arrays for the current timestep. Should be called only once per timestep.
        """
        return {name: await array.get_full_array() for name, array in self.arrays.items()}


async def start(simulation_callback, arrays_description: list[DaskArrayInfo]) -> None:
    # The workers will be able to access to this actor using its name
    head: Any = SimulationHead.options(
        name="simulation_head",
        namespace="doreisa",
        # Schedule the actor on this node
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=ray.get_runtime_context().get_node_id(),
            soft=False,
        ),
    ).remote(arrays_description)

    print("Waiting to start the simulation...")

    step = 0

    while True:
        all_arrays: dict[str, da.Array] = ray.get(head.get_all_arrays.remote())

        if step == 0:
            print("Simulation started!")

        simulation_callback(**all_arrays, timestep=step)
        step += 1

        head.simulation_step_complete.remote()
