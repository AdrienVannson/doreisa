import asyncio
import pickle
from dataclasses import dataclass

import numpy as np
import ray
import ray.actor
import ray.util.dask.scheduler

from doreisa import Timestep


@dataclass
class ChunkRef:
    """
    Represents a chunk of an array in a Dask task graph.

    The task corresping to this object must be scheduled by the actor who has the actual
    data. This class is used since Dask tends to inline simple tuples. This may change
    in newer versions of Dask.
    """

    actor_id: int
    array_name: str  # The real name, without the timestep
    timestep: Timestep
    position: tuple[int, ...]

    # Set for one chunk only.
    _all_chunks: ray.ObjectRef | None = None


@dataclass
class ScheduledByOtherActor:
    """
    Represents a task that is scheduled by another actor in the part of the task graph sent to an actor.
    """

    actor_id: int


class GraphInfo:
    """
    Information about graphs and their scheduling.
    """

    def __init__(self):
        self.scheduled_event = asyncio.Event()
        self.refs: dict[str, ray.ObjectRef] = {}


@dataclass
class ChunkReadyInfo:
    # Information about the array
    array_name: str
    timestep: Timestep
    dtype: np.dtype
    nb_chunks_per_dim: tuple[int, ...]

    # Information about the chunk
    position: tuple[int, ...]
    size: tuple[int, ...]


@ray.remote(num_cpus=0, enable_task_events=False)
def patched_dask_task_wrapper(func, repack, key, ray_pretask_cbs, ray_posttask_cbs, *args, first_call=True):
    """
    Patched version of the original dask_task_wrapper function.

    This version received ObjectRefs first, and calls itself a second time to unwrap the ObjectRefs.
    The result is an ObjectRef.

    TODO can probably be rewritten without copying the whole function
    """

    if first_call:
        assert all([isinstance(a, ray.ObjectRef) for a in args])
        # Use one CPU for the actual computation
        return patched_dask_task_wrapper.options(num_cpus=1).remote(
            func, repack, key, ray_pretask_cbs, ray_posttask_cbs, *args, first_call=False
        )

    if ray_pretask_cbs is not None:
        pre_states = [cb(key, args) if cb is not None else None for cb in ray_pretask_cbs]
    repacked_args, repacked_deps = repack(args)
    # Recursively execute Dask-inlined tasks.
    actual_args = [ray.util.dask.scheduler._execute_task(a, repacked_deps) for a in repacked_args]
    # Execute the actual underlying Dask task.
    result = func(*actual_args)

    if ray_posttask_cbs is not None:
        for cb, pre_state in zip(ray_posttask_cbs, pre_states):
            if cb is not None:
                cb(key, result, pre_state)

    return result


@ray.remote(num_cpus=0, enable_task_events=False)
def remote_ray_dask_get(dsk, keys):
    import ray.util.dask

    # Monkey-patch Dask-on-Ray
    ray.util.dask.scheduler.dask_task_wrapper = patched_dask_task_wrapper

    return ray.util.dask.ray_dask_get(dsk, keys, ray_persist=True)


@ray.remote
class SchedulingActor:
    """
    Actor in charge of gathering ObjectRefs and scheduling the tasks produced by the head node.
    """

    def __init__(self, actor_id: int) -> None:
        self.actor_id = actor_id
        self.actor_handle = ray.get_runtime_context().current_actor

        self.head = ray.get_actor("simulation_head", namespace="doreisa")
        self.scheduling_actors: list[ray.actor.ActorHandle] = []

        # For collecting chunks

        # Triggered when all the chunks are ready
        self.chunks_ready_event = asyncio.Event()

        self.chunks_info: dict[str, list[ChunkReadyInfo]] = {}

        # (dask_array_name, position) -> chunk
        # The Dask array name contains the timestep
        self.local_chunks: dict[tuple[str, Timestep, tuple[int, ...]], ray.ObjectRef | bytes] = {}

        # For scheduling
        self.new_graph_available = asyncio.Event()
        self.graph_infos: dict[int, GraphInfo] = {}
        self.partitionned_graphs: dict[int, dict] = {}

    def ready(self) -> None:
        pass

    def _pack_object_ref(self, refs: list[ray.ObjectRef]):
        """
        Used to create an ObjectRef containing the given ObjectRef.
        This allows having the expected format in the task graph.

        This is a method instead of a function with `num_cpus=0` to avoid starting many
        new workers.
        """
        return refs[0]

    async def add_chunk(
        self,
        array_name: str,
        timestep: int,
        chunk_position: tuple[int, ...],
        dtype: np.dtype,
        nb_chunks_per_dim: tuple[int, ...],
        nb_chunks_of_node: int,
        chunk: list[ray.ObjectRef],
        chunk_shape: tuple[int, ...],
    ) -> None:
        assert (array_name, timestep, chunk_position) not in self.local_chunks

        self.local_chunks[(array_name, timestep, chunk_position)] = self.actor_handle._pack_object_ref.remote(chunk)

        if array_name not in self.chunks_info:
            self.chunks_info[array_name] = []
        chunks_info = self.chunks_info[array_name]

        chunks_info.append(
            ChunkReadyInfo(
                array_name=array_name,
                timestep=timestep,
                dtype=dtype,
                nb_chunks_per_dim=nb_chunks_per_dim,
                position=chunk_position,
                size=chunk_shape,
            )
        )

        if len(chunks_info) == nb_chunks_of_node:
            chunks = []
            for info in chunks_info:
                c = self.local_chunks[(info.array_name, info.timestep, info.position)]
                assert isinstance(c, ray.ObjectRef)
                chunks.append(c)
                self.local_chunks[(info.array_name, info.timestep, info.position)] = pickle.dumps(c)

            all_chunks_ref = ray.put(chunks)

            await self.head.chunks_ready.options(enable_task_events=False).remote(
                chunks_info, self.actor_id, [all_chunks_ref]
            )
            self.chunks_info[array_name] = []
            self.chunks_ready_event.set()
            self.chunks_ready_event.clear()
        else:
            await self.chunks_ready_event.wait()

    def store_graph(self, graph_id: int, dsk: dict) -> None:
        """
        Store the given graph in the actor until `schedule_graph` is called.

        This allows measuring precisely the time it takes to send the graph to all the
        actors. If needed, this will be optimized using an efficient communication
        method.
        """
        self.partitionned_graphs[graph_id] = dsk

    async def schedule_graph(self, graph_id: int):
        dsk = self.partitionned_graphs.pop(graph_id)

        # Find the scheduling actors
        if not self.scheduling_actors:
            self.scheduling_actors = await self.head.list_scheduling_actors.options(enable_task_events=False).remote()

        info = GraphInfo()
        self.graph_infos[graph_id] = info
        self.new_graph_available.set()
        self.new_graph_available.clear()

        for key, val in dsk.items():
            # Adapt external keys
            if isinstance(val, ScheduledByOtherActor):
                actor = self.scheduling_actors[val.actor_id]
                dsk[key] = actor.get_value.options(enable_task_events=False).remote(graph_id, key)

            # Replace the false chunks by the real ObjectRefs
            if isinstance(val, ChunkRef):
                assert val.actor_id == self.actor_id

                encoded_ref = self.local_chunks[(val.array_name, val.timestep, val.position)]
                assert isinstance(encoded_ref, bytes)
                dsk[key] = pickle.loads(encoded_ref)

        # We will need the ObjectRefs of these keys
        keys_needed = list(dsk.keys())

        refs = await remote_ray_dask_get.remote(dsk, keys_needed)

        for key, ref in zip(keys_needed, refs):
            info.refs[key] = ref

        info.scheduled_event.set()

    async def get_value(self, graph_id: int, key: str):
        while graph_id not in self.graph_infos:
            await self.new_graph_available.wait()

        await self.graph_infos[graph_id].scheduled_event.wait()
        return await self.graph_infos[graph_id].refs[key]
