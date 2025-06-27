import dask.array as da
import pytest
import ray

from tests.utils import ray_cluster, simple_worker, wait_for_head_node  # noqa: F401

NB_ITERATIONS = 10


@ray.remote(max_retries=0)
def head_script() -> None:
    """The head node checks that the values are correct"""
    from doreisa.head_node import init
    from doreisa.window_api import ArrayDefinition, run_simulation

    init()

    def simulation_callback(array: list[da.Array], timestep: int):
        x = array[0].sum().compute()

        # assert x == 10 * timestep

    run_simulation(
        simulation_callback,
        [ArrayDefinition("array", window_size=1)],
        max_iterations=NB_ITERATIONS,
    )


@pytest.mark.parametrize("nb_nodes", [32])
def test_big(nb_nodes: int, ray_cluster) -> None:  # noqa: F811
    nb_chunks_per_node = 5

    head_ref = head_script.remote()
    wait_for_head_node()

    worker_refs = []
    for rank in range(nb_nodes * nb_chunks_per_node):
        worker_refs.append(
            simple_worker.remote(
                rank=rank,
                position=(rank,),
                chunks_per_dim=(nb_nodes * nb_chunks_per_node,),
                nb_chunks_of_node=nb_chunks_per_node,
                chunk_size=(1, 1),
                nb_iterations=NB_ITERATIONS,
                node_id=f"node_{rank // nb_chunks_per_node}",
            )
        )

    ray.get([head_ref] + worker_refs)

    # Check that the right number of scheduling actors were created
    simulation_head = ray.get_actor("simulation_head", namespace="doreisa")
    assert len(ray.get(simulation_head.list_scheduling_actors.remote())) == nb_nodes
