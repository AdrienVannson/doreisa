import asyncio
import math
from dataclasses import dataclass
from typing import Any, Callable

import dask
import dask.array as da
import numpy as np
import ray
import ray.actor
from dask.highlevelgraph import HighLevelGraph
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from doreisa._scheduler import doreisa_get
from doreisa._scheduling_actor import ChunkReadyInfo, SchedulingActor


def init():
    if not ray.is_initialized():
        ray.init(address="auto")

    dask.config.set(scheduler=doreisa_get, shuffle="tasks")


@dataclass
class DaskArrayInfo:
    """
    Description of a Dask array given by the user.
    """

    name: str
    window_size: int = 1
    preprocess: Callable = lambda x: x


class _DaskArrayData:
    """
    All the information concerning the Dask array needed during the simulation (chunks,
    synchronization, etc.).
    """

    def __init__(self, description: DaskArrayInfo) -> None:
        self.description = description

        # This will be set when the first chunk is added
        self.nb_chunks_per_dim: tuple[int, ...] | None = None
        self.nb_chunks: int | None = None

        # For each dimension, the size of the chunks in this dimension
        self.chunks_size: list[list[int | None]] | None = None

        # Timestep of the array currently being built
        self.timestep: int = 0

        # ID of the scheduling actor in charge of the chunk at each position
        self.scheduling_actors_id: dict[tuple[int, ...], int] = {}

        # Moving window of the full arrays for the previous timesteps
        self.full_arrays: list[da.Array] = []

        # Event sent when a full array is built
        self.array_built = asyncio.Event()

        # Event sent each time a chunk is added
        self.chunk_added = asyncio.Event()

    async def add_chunk(
        self,
        size: tuple[int, ...],
        position: tuple[int, ...],
        nb_chunks_per_dim: tuple[int, ...],
        scheduling_actor_id: int,
    ) -> None:
        if self.nb_chunks_per_dim is None:
            self.nb_chunks_per_dim = nb_chunks_per_dim
            self.nb_chunks = math.prod(nb_chunks_per_dim)

            self.chunks_size = [[None for _ in range(n)] for n in nb_chunks_per_dim]
        else:
            assert self.nb_chunks_per_dim == nb_chunks_per_dim
            assert self.chunks_size is not None

        for pos, nb_chunks in zip(position, nb_chunks_per_dim):
            assert 0 <= pos < nb_chunks

        if position in self.scheduling_actors_id:
            await self.array_built.wait()

            # TODO can we always make this assumption?
            assert position not in self.scheduling_actors_id

        self.scheduling_actors_id[position] = scheduling_actor_id

        for d in range(len(position)):
            if self.chunks_size[d][position[d]] is None:
                self.chunks_size[d][position[d]] = size[d]
            else:
                assert self.chunks_size[d][position[d]] == size[d]

        # Inform that the data is available
        self.chunk_added.set()

    async def get_full_array(self) -> da.Array:
        """
        Return the full array for the current timestep.
        """
        # Wait until all the data for the step is available
        while self.nb_chunks is None or len(self.scheduling_actors_id) < self.nb_chunks:
            await self.chunk_added.wait()
            self.chunk_added.clear()

        assert self.nb_chunks_per_dim is not None

        # We need to add the timestep since the same name can be used several times for different
        # timesteps
        name = f"{self.description.name}_{self.timestep}"

        graph = {
            # Adding the id function prevents inlining the value
            (name,) + position: ("doreisa_chunk", actor_id)
            for position, actor_id in self.scheduling_actors_id.items()
        }

        dsk = HighLevelGraph.from_collections(name, graph, dependencies=())

        full_array = da.Array(
            dsk,
            name,
            chunks=self.chunks_size,
            dtype=np.float64,
        )

        # Reset the chunks for the next timestep
        self.scheduling_actors_id = {}
        self.array_built.set()
        self.array_built.clear()
        self.timestep += 1

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
        # For each ID of a simulation node, the corresponding scheduling actor
        self.scheduling_actors: dict[str, ray.actor.ActorHandle] = {}

        # For each name, the corresponding array
        # Python dictionnaries preserve the insertion order. The ID of the i-th actor is i.
        self.arrays: dict[str, _DaskArrayData] = {
            description.name: _DaskArrayData(description) for description in arrays_description
        }

    def list_scheduling_actors(self) -> list[ray.actor.ActorHandle]:
        """
        Return the list of scheduling actors.
        """
        return list(self.scheduling_actors.values())

    async def scheduling_actor(self, node_id: str, *, is_fake_id: bool = False) -> ray.actor.ActorHandle:
        """
        Return the scheduling actor for the given node ID.

        Args:
            node_id: The ID of the node.
            is_fake_id: If True, the ID isn't a Ray node ID, and the actor can be scheduled
                anywhere. This is useful for testing purposes.
        """

        if node_id not in self.scheduling_actors:
            actor_id = len(self.scheduling_actors)

            if is_fake_id:
                self.scheduling_actors[node_id] = SchedulingActor.remote(actor_id)  # type: ignore
            else:
                self.scheduling_actors[node_id] = SchedulingActor.options(  # type: ignore
                    # Schedule the actor on this node
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        node_id=node_id,
                        soft=False,
                    ),
                    # Prevents the actor from being stuck
                    max_concurrency=1000_000_000,
                ).remote(actor_id)

            await self.scheduling_actors[node_id].ready.remote()  # type: ignore

        return self.scheduling_actors[node_id]

    def preprocessing_callbacks(self) -> dict[str, Callable]:
        """
        Return the preprocessing callbacks for each array.
        """
        return {name: array.description.preprocess for name, array in self.arrays.items()}

    async def chunks_ready(self, chunks: list[ChunkReadyInfo], scheduling_actor_id: int) -> None:
        """
        Called by the scheduling actors to inform the head actor that the chunks are ready.
        The chunks are not sent.

        Args:
            chunks: Information about the chunks that are ready.
            source_actor: Handle to the scheduling actor owning the chunks.
        """
        for chunk in chunks:
            await self.arrays[chunk.array_name].add_chunk(
                chunk.size, chunk.position, chunk.nb_chunks_per_dim, scheduling_actor_id
            )

    async def get_all_arrays(self) -> dict[str, list[da.Array]]:
        """
        Return all the arrays for the current timestep. Should be called only once per timestep.
        """
        return {name: await array.get_full_array_hist() for name, array in self.arrays.items()}


async def start(simulation_callback, arrays_description: list[DaskArrayInfo], *, max_iterations=1000_000_000) -> None:
    # The workers will be able to access to this actor using its name
    head: Any = SimulationHead.options(
        name="simulation_head",
        namespace="doreisa",
        # Schedule the actor on this node
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=ray.get_runtime_context().get_node_id(),
            soft=False,
        ),
        # Prevents the actor from being stuck when it needs to gather many refs
        max_concurrency=1000_000_000,
        # Prevents the actor from being deleted when the function ends
        lifetime="detached",
    ).remote(arrays_description)

    print("Waiting to start the simulation...")

    for step in range(max_iterations):
        all_arrays: dict[str, da.Array] = ray.get(head.get_all_arrays.remote())

        if step == 0:
            print("Simulation started!")

        simulation_callback(**all_arrays, timestep=step)
