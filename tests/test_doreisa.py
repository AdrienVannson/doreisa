import asyncio
import pytest
import multiprocessing as mp
import time
import dask.array as da

import ray
import ray.util.state
from tests.utils import ray_cluster, simple_worker  # noqa: F401


NB_ITERATIONS = 10


def head() -> None:
    """The head node checks that the values are correct"""
    import doreisa.head_node as doreisa

    doreisa.init()

    def simulation_callback(array: list[da.Array], timestep: int):
        x = array[0].sum().compute()

        if x != 10 * timestep:
            exit(1)

        if timestep == NB_ITERATIONS - 1:
            exit(0)

    asyncio.run(
        doreisa.start(
            simulation_callback,
            [
                doreisa.DaskArrayInfo("array", window_size=1),
            ],
        )
    )

@pytest.mark.parametrize("nb_nodes", [1, 2, 4])
def test_doreisa(nb_nodes: int, ray_cluster) -> None:  # noqa: F811
    head_process = mp.Process(target=head, daemon=True)
    head_process.start()

    time.sleep(5)

    worker_processes = []
    for rank in range(4):
        worker_process = mp.Process(
            target=simple_worker,
            args=(rank, (rank // 2, rank % 2), (2, 2), (1, 1), NB_ITERATIONS),
            kwargs={"node_id": f"node_{rank % nb_nodes}"},
            daemon=True,
        )
        worker_process.start()
        worker_processes.append(worker_process)

    time.sleep(2)

    # Check that the right number of scheduling actors were created
    # ray.init(address="auto")
    # simulation_head = ray.get_actor("simulation_head", namespace="doreisa")
    # assert ray.get(simulation_head.nb_scheduling_actors.remote()) == nb_nodes

    head_process.join(timeout=10)
    assert head_process.exitcode == 0
