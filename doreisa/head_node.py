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

        # Chunks of the array being currently built
        self.chunks: dict[tuple[int, ...], da.Array] = {}

        # Moving window of the full arrays for the previous timesteps
        self.full_arrays: list[da.Array] = []

        # Event sent when a full array is built
        self.array_built = asyncio.Event()

        # Event sent each time a chunk is added
        self.chunk_added = asyncio.Event()

    async def add_chunk(self, position: tuple[int, ...], chunk: da.Array) -> None:
        if position in self.chunks:
            await self.array_built.wait()
            assert position not in self.chunks

        self.chunks[position] = chunk

        # Inform that the data is available
        self.chunk_added.set()

    async def get_full_array(self) -> da.Array:
        """
        Return the full array for the current timestep.
        """
        # Wait until all the data for the step is available
        while len(self.chunks) < self.nb_chunks:
            await self.chunk_added.wait()
            self.chunk_added.clear()

        def create_blocks(shape: tuple[int, ...], position: tuple[int, ...]=()):
            # Recursively create the blocks
            if not shape:
                return self.chunks[position]

            return [create_blocks(shape[1:], position + (i,)) for i in range(shape[0])]

        # Return the complete grid
        full_array = da.block(create_blocks(self.description.nb_chunks_per_dim))

        # Reset the chunks for the next timestep
        self.chunks = {}
        self.array_built.set()
        self.array_built.clear()

        return full_array

    async def get_full_array_hist(self) -> list[da.Array]:
        """
        Return a list of size up to `window_size` with the full arrays for the previous timesteps.
        """
        if len(self.full_arrays) == self.description.window_size:
            self.full_arrays = self.full_arrays[1:]
        self.full_arrays.append(await self.get_full_array())
        return self.full_arrays


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

    async def add_chunk(self, array_name: str, position: tuple[int, ...], chunk_ray: list[ray.ObjectRef], chunk_size: tuple[int, ...]) -> None:
        # Putting the chunk in a list prevents ray from dereferencing the object.

        # Convert to a dask array
        chunk: da.Array = da.from_delayed(ray_to_dask(chunk_ray[0]), chunk_size, dtype=float)

        await self.arrays[array_name].add_chunk(position, chunk)

    async def get_all_arrays(self) -> dict[str, list[da.Array]]:
        """
        Return all the arrays for the current timestep. Should be called only once per timestep.
        """
        return {name: await array.get_full_array_hist() for name, array in self.arrays.items()}


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
