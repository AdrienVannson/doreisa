import asyncio

import ray
import ray.actor
from dask.core import get_dependencies


class GraphInfo:
    """
    Information about graphs and their scheduling.
    """

    def __init__(self):
        self.key_ready_events: dict[str, asyncio.Event] = {}
        self.refs: dict[str, ray.ObjectRef] = {}


@ray.remote
def remote_ray_dask_get(dsk, keys):
    import ray.util.dask
    import ray.util.dask.scheduler

    @ray.remote
    def patched_dask_task_wrapper(func, repack, key, ray_pretask_cbs, ray_posttask_cbs, *args, first_call=True):
        """
        Patched version of the original dask_task_wrapper function.

        This version received ObjectRefs first, and calls itself a second time to unwrap the ObjectRefs.
        The result is an ObjectRef.

        TODO can probably be rewritten without copying the whole function
        """

        if first_call:
            assert all([isinstance(a, ray.ObjectRef) for a in args])
            return patched_dask_task_wrapper.remote(
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

        self.new_graph_available = asyncio.Event()
        self.graph_infos: dict[int, GraphInfo] = {}

        self.head = ray.get_actor("simulation_head", namespace="doreisa")
        self.scheduling_actors: list[ray.actor.ActorHandle] = []

    def ready(self) -> None:
        pass

    async def schedule_graph(self, dsk: dict, graph_id: int, scheduling: dict[str, int]):
        # Find the scheduling actors
        if not self.scheduling_actors:
            self.scheduling_actors = await self.head.list_scheduling_actors.remote()

        info = GraphInfo()
        self.graph_infos[graph_id] = info
        self.new_graph_available.set()
        self.new_graph_available.clear()

        local_keys = {k for k in dsk if scheduling[k] == self.actor_id}

        dependency_keys: set[str] = {dep for k in local_keys for dep in get_dependencies(dsk, k)}  # type: ignore[assignment]

        external_keys = dependency_keys - local_keys

        # Filter the dask array
        dsk = {k: v for k, v in dsk.items() if k in local_keys}

        # Adapt external keys
        for k in external_keys:
            actor = self.scheduling_actors[scheduling[k]]
            dsk[k] = actor.get_value.remote(graph_id, k)

        # Prepare key events
        for k in local_keys - dependency_keys:  # TODO doesn't always work
            info.key_ready_events[k] = asyncio.Event()
        keys_with_events = list(local_keys - dependency_keys)

        refs = await remote_ray_dask_get.remote(dsk, keys_with_events)

        for key, ref in zip(keys_with_events, refs):
            info.refs[key] = ref
            info.key_ready_events[key].set()

    async def get_value(self, graph_id: int, key: str):
        while graph_id not in self.graph_infos:
            await self.new_graph_available.wait()

        assert key in self.graph_infos[graph_id].key_ready_events

        await self.graph_infos[graph_id].key_ready_events[key].wait()
        return await self.graph_infos[graph_id].refs[key]

    # TODO
    # async def terminate_graph(self, graph_id: int):
    #     while graph_id not in self.graph_infos:
    #         await self.new_graph_available.wait()

    #     del self.graph_infos[graph_id]
