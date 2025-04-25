import asyncio

import ray
import ray.actor
from dask.core import get_dependencies
from ray.util.dask import ray_dask_get


class GraphInfo:
    """
    Information about graphs and their scheduling.
    """

    def __init__(self):
        self.key_ready_events: dict[str, asyncio.Event] = {}
        self.refs: dict[str, ray.ObjectRef] = {}


def task_ready(actor, graph_id: int, key, value):
    """
    Task executed when the data is ready.
    """
    assert key[0] == "key"
    key = key[1]

    actor.set_task_ready.remote(graph_id, key, [value])


@ray.remote
def remote_ray_dask_get(dsk):
    ray_dask_get(dsk, [])


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

        # Add tasks executed when data is ready
        data_ready_tasks = {}
        for k in local_keys - dependency_keys:  # TODO same
            data_ready_tasks[("dask_on_ray_ready", k)] = (
                task_ready,
                self.scheduling_actors[self.actor_id],
                graph_id,
                ("key", k),
                k,
            )

        dsk.update(data_ready_tasks)

        await remote_ray_dask_get.remote(dsk)

    def set_task_ready(self, graph_id: int, key: str, ref: list[ray.ObjectRef]):
        self.graph_infos[graph_id].refs[key] = ref[0]
        self.graph_infos[graph_id].key_ready_events[key].set()

    async def get_value(self, graph_id: int, key: str):
        while graph_id not in self.graph_infos:
            await self.new_graph_available.wait()

        assert key in self.graph_infos[graph_id].key_ready_events

        await self.graph_infos[graph_id].key_ready_events[key].wait()
        return self.graph_infos[graph_id].refs[key]

    # TODO
    # async def terminate_graph(self, graph_id: int):
    #     while graph_id not in self.graph_infos:
    #         await self.new_graph_available.wait()

    #     del self.graph_infos[graph_id]
