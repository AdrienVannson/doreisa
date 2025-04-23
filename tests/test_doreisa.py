import asyncio

import dask.array as da
import pytest
import ray

from tests.utils import ray_cluster, simple_worker, wait_for_head_node  # noqa: F401

NB_ITERATIONS = 10


@ray.remote
def head_script() -> None:
    """The head node checks that the values are correct"""
    import doreisa.head_node as doreisa

    doreisa.init()

    def simulation_callback(array: list[da.Array], timestep: int):
        x = array[0].sum().compute()

        assert x == 10 * timestep

    asyncio.run(
        doreisa.start(
            simulation_callback,
            [
                doreisa.DaskArrayInfo("array", window_size=1),
            ],
            max_iterations=NB_ITERATIONS,
        )
    )


def check_scheduling_actors(nb_actors: int) -> None:
    """Check that the right number of scheduling actors were created"""
    ray.init(address="auto")

    simulation_head = ray.get_actor("simulation_head", namespace="doreisa")
    assert len(ray.get(simulation_head.list_scheduling_actors.remote())) == nb_actors


@pytest.mark.parametrize("nb_nodes", [1, 2, 4])
def test_doreisa(nb_nodes: int, ray_cluster) -> None:  # noqa: F811
    head_ref = head_script.remote()
    wait_for_head_node()

    worker_refs = []
    for rank in range(4):
        worker_refs.append(
            simple_worker.remote(
                rank, (rank // 2, rank % 2), (2, 2), (1, 1), NB_ITERATIONS, node_id=f"node_{rank % nb_nodes}"
            )
        )

    ray.get(worker_refs)
    ray.get(head_ref)

    # Check that the right number of scheduling actors were created
    simulation_head = ray.get_actor("simulation_head", namespace="doreisa")
    assert len(ray.get(simulation_head.list_scheduling_actors.remote())) == nb_nodes
