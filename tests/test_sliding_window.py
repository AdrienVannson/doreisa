import asyncio

import dask.array as da
import ray
from ray.util.dask import enable_dask_on_ray

import doreisa.head_node as doreisa
from tests.utils import ray_cluster, simple_worker, wait_for_head_node  # noqa: F401

NB_ITERATIONS = 10


@ray.remote
def head() -> None:
    """The head node checks that the values are correct"""
    enable_dask_on_ray()

    def simulation_callback(array: list[da.Array], timestep: int):
        if timestep == 0:
            assert len(array) == 1
            return

        assert array[0].sum().compute() == 10 * (timestep - 1)
        assert array[1].sum().compute() == 10 * timestep

        # Test a computation where the two arrays are used at the same time.
        # This checks that they are defined with different names.
        assert (array[1] - array[0]).sum().compute() == 10

    asyncio.run(
        doreisa.start(
            simulation_callback,
            [
                doreisa.DaskArrayInfo("array", window_size=2),
            ],
            max_iterations=NB_ITERATIONS,
        )
    )


def test_sliding_window(ray_cluster) -> None:  # noqa: F811
    head_ref = head.remote()
    wait_for_head_node()

    worker_refs = []
    for rank in range(4):
        worker_refs.append(
            simple_worker.remote(rank, (rank // 2, rank % 2), (2, 2), (1, 1), NB_ITERATIONS, node_id=f"node_{rank}")
        )

    ray.get(worker_refs)
    ray.get(head_ref)
