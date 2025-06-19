import asyncio
import logging
import math
from dataclasses import dataclass
from typing import Callable

import dask
import dask.array as da
import numpy as np
import ray
import ray.actor
from dask.highlevelgraph import HighLevelGraph
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from doreisa import Timestep
from doreisa._scheduler import doreisa_get
from doreisa._scheduling_actor import ChunkReadyInfo, ChunkRef, SchedulingActor


def init():
    if not ray.is_initialized():
        ray.init(address="auto", log_to_driver=False, logging_level=logging.ERROR)

    dask.config.set(scheduler=doreisa_get, shuffle="tasks")


@dataclass
class ArrayDefinition:
    """
    Description of a Dask array given by the user.
    """

    name: str
    preprocess: Callable = lambda x: x


class _DaskArrayData:
    """
    Information about a Dask array being built.
    """

    def __init__(self, definition: ArrayDefinition, timestep: Timestep) -> None:
        self.definition = definition
        self.timestep = timestep

        # This will be set when the first chunk is added
        self.nb_chunks_per_dim: tuple[int, ...] | None = None
        self.nb_chunks: int | None = None

        # For each dimension, the size of the chunks in this dimension
        self.chunks_size: list[list[int | None]] | None = None

        # Type of the numpy arrays
        self.dtype: np.dtype | None = None

        # ID of the scheduling actor in charge of the chunk at each position
        self.scheduling_actors_id: dict[tuple[int, ...], int] = {}

        # Each reference comes from one scheduling actor. The reference a list of
        # ObjectRefs, each ObjectRef corresponding to a chunk. These references
        # shouldn't be used directly. They exists only to release the memory
        # automatically.
        # When the array is buit, these references are put in the object store, and the
        # global reference is added to the Dask graph. Then, the list is cleared.
        self.chunk_refs: list[ray.ObjectRef] = []

    def add_chunk(
        self,
        size: tuple[int, ...],
        position: tuple[int, ...],
        dtype: np.dtype,
        nb_chunks_per_dim: tuple[int, ...],
        scheduling_actor_id: int,
    ) -> bool:
        """
        Add a chunk to the array.

        Return:
            True if the array is ready, False otherwise.
        """
        if self.nb_chunks_per_dim is None:
            self.nb_chunks_per_dim = nb_chunks_per_dim
            self.nb_chunks = math.prod(nb_chunks_per_dim)

            self.dtype = dtype
            self.chunks_size = [[None for _ in range(n)] for n in nb_chunks_per_dim]
        else:
            assert self.nb_chunks_per_dim == nb_chunks_per_dim
            assert self.dtype == dtype
            assert self.chunks_size is not None

        for pos, nb_chunks in zip(position, nb_chunks_per_dim):
            assert 0 <= pos < nb_chunks

        self.scheduling_actors_id[position] = scheduling_actor_id

        for d in range(len(position)):
            if self.chunks_size[d][position[d]] is None:
                self.chunks_size[d][position[d]] = size[d]
            else:
                assert self.chunks_size[d][position[d]] == size[d]

        if len(self.scheduling_actors_id) == self.nb_chunks:  # The array is ready
            return True
        return False

    def add_chunk_ref(self, chunk_ref: ray.ObjectRef) -> None:
        self.chunk_refs.append(chunk_ref)

    def get_full_array(self) -> da.Array:
        """
        Return the full Dask array.
        """
        assert len(self.scheduling_actors_id) == self.nb_chunks
        assert self.nb_chunks is not None and self.nb_chunks_per_dim is not None

        all_chunks = ray.put(self.chunk_refs)

        # We need to add the timestep since the same name can be used several times for different
        # timesteps
        dask_name = f"{self.definition.name}_{self.timestep}"

        graph = {
            # We need to repeat the name and position in the value since the key might be removed
            # by the Dask optimizer
            (dask_name,) + position: ChunkRef(
                actor_id, self.definition.name, self.timestep, position, _all_chunks=all_chunks if it == 0 else None
            )
            for it, (position, actor_id) in enumerate(self.scheduling_actors_id.items())
        }

        dsk = HighLevelGraph.from_collections(dask_name, graph, dependencies=())

        full_array = da.Array(
            dsk,
            dask_name,
            chunks=self.chunks_size,
            dtype=self.dtype,
        )

        return full_array


def get_head_actor_options() -> dict:
    """Return the options that should be used to start the head actor."""
    return dict(
        # The workers will be able to access to this actor using its name
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
        # Disabled for performance reasons
        enable_task_events=False,
    )


@ray.remote
class SimulationHead:
    def __init__(self, arrays_definitions: list[ArrayDefinition], max_pending_arrays: int = 1_000_000_000) -> None:
        """
        Initialize the simulation head.

        Args:
            arrays_description: Description of the arrays to be created.
            max_pending_arrays: Maximum number of arrays that can be being built or
                waiting to be collected at the same time. Setting the value can prevent
                the simulation to be many iterations in advance of the analytics.
        """

        # For each ID of a simulation node, the corresponding scheduling actor
        self.scheduling_actors: dict[str, ray.actor.ActorHandle] = {}

        self.arrays_definition: dict[str, ArrayDefinition] = {
            definition.name: definition for definition in arrays_definitions
        }

        # Must be used before creating a new array
        self.new_pending_array_semaphore = asyncio.Semaphore(max_pending_arrays)

        # Triggered when a new array is added to self.arrays
        self.new_array_created = asyncio.Event()

        # Arrays beeing built
        self.arrays: dict[tuple[str, Timestep], _DaskArrayData] = {}

        # All the newly created arrays
        self.arrays_ready: asyncio.Queue[tuple[str, int, da.Array]] = asyncio.Queue()

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
                    max_concurrency=1000_000_000,  # Prevents the actor from being stuck
                    num_cpus=0,
                    enable_task_events=False,
                ).remote(actor_id)

        await self.scheduling_actors[node_id].ready.remote()  # type: ignore
        return self.scheduling_actors[node_id]

    def preprocessing_callbacks(self) -> dict[str, Callable]:
        """
        Return the preprocessing callbacks for each array.
        """
        return {name: definition.preprocess for name, definition in self.arrays_definition.items()}

    async def chunks_ready(
        self, chunks: list[ChunkReadyInfo], scheduling_actor_id: int, all_chunks_ref: list[ray.ObjectRef]
    ) -> None:
        """
        Called by the scheduling actors to inform the head actor that the chunks are ready.
        The chunks are not sent.

        Args:
            chunks: Information about the chunks that are ready.
            source_actor: Handle to the scheduling actor owning the chunks.
        """
        for it, chunk in enumerate(chunks):
            while (chunk.array_name, chunk.timestep) not in self.arrays:
                t1 = asyncio.create_task(self.new_pending_array_semaphore.acquire())
                t2 = asyncio.create_task(self.new_array_created.wait())

                done, pending = await asyncio.wait([t1, t2], return_when=asyncio.FIRST_COMPLETED)

                for task in pending:
                    task.cancel()

                if t1 in done:
                    if (chunk.array_name, chunk.timestep) in self.arrays:
                        # The array was already created by another scheduling actor
                        self.new_pending_array_semaphore.release()
                    else:
                        self.arrays[(chunk.array_name, chunk.timestep)] = _DaskArrayData(
                            self.arrays_definition[chunk.array_name], chunk.timestep
                        )

                        self.new_array_created.set()
                        self.new_array_created.clear()

            array = self.arrays[(chunk.array_name, chunk.timestep)]

            # TODO refactor so that the function works with only one array
            if it == 0:
                array.add_chunk_ref(all_chunks_ref[0])

            is_ready = array.add_chunk(
                chunk.size, chunk.position, chunk.dtype, chunk.nb_chunks_per_dim, scheduling_actor_id
            )

            if is_ready:
                self.arrays_ready.put_nowait(
                    (
                        chunk.array_name,
                        array.timestep,
                        array.get_full_array(),
                    )
                )
                del self.arrays[(chunk.array_name, chunk.timestep)]

    async def get_next_array(self) -> tuple[str, int, da.Array]:
        array = await self.arrays_ready.get()
        self.new_pending_array_semaphore.release()
        return array
