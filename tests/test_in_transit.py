import dask.array as da
import pytest
import ray
import ray.actor

from tests.utils import in_transit_worker, ray_cluster, wait_for_head_node  # noqa: F401

NB_ITERATIONS = 10


@ray.remote(max_retries=0)
def head_script() -> None:
    """The head node checks that the values are correct"""
    from doreisa.head_node import init
    from doreisa.window_api import ArrayDefinition, run_simulation

    init()

    def simulation_callback(array: da.Array, timestep: int):
        x = array.sum().compute()

        assert x == 10 * timestep

    run_simulation(
        simulation_callback,
        [ArrayDefinition("array")],
        max_iterations=NB_ITERATIONS,
    )


@pytest.mark.parametrize(
    "nb_simulation_nodes, nb_analytic_nodes",
    [(1, 1), (2, 2), (2, 4), (4, 2), (4, 4)],
)
def test_in_transit(nb_simulation_nodes: int, nb_analytic_nodes: int, ray_cluster) -> None:  # noqa: F811
    from doreisa.in_transit_analytic_actor import InTransitAnalyticActor

    # Start the head actor
    head_ref = head_script.remote()
    wait_for_head_node()

    # Start the analytic actors
    analytic_actors: list[ray.actor.ActorHandle] = []
    for i in range(nb_analytic_nodes):
        actor = InTransitAnalyticActor.remote(_fake_node_id=f"node_{i}")

        for j in range(4 // nb_analytic_nodes):
            actor.run_zmq_server.remote(f"localhost:{8000 + i * (4 // nb_analytic_nodes) + j}")

        analytic_actors.append(actor)

    worker_refs = []
    for rank in range(4):
        worker_refs.append(
            in_transit_worker.remote(
                rank=rank,
                position=(rank // 2, rank % 2),
                chunks_per_dim=(2, 2),
                nb_chunks_of_analytic_node=4 // nb_analytic_nodes,
                chunk_size=(1, 1),
                nb_iterations=NB_ITERATIONS,
                analytic_node_address=f"localhost:{8000 + rank}",
            )
        )

    ray.get([head_ref] + worker_refs)

    # Check that the right number of scheduling actors were created
    simulation_head = ray.get_actor("simulation_head", namespace="doreisa")
    assert len(ray.get(simulation_head.list_scheduling_actors.remote())) == nb_analytic_nodes


if __name__ == "__main__":
    ray.init()
    test_in_transit(4, 2, None)
