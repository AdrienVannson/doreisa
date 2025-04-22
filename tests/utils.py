import ray
import pytest
import time


@pytest.fixture
def ray_cluster():
    """Start a Ray cluster for this test"""
    ray.init(num_cpus=100)

    yield

    ray.shutdown()


def wait_for_head_node() -> None:
    """Wait until the head node is ready"""
    while True:
        try:
            ray.get_actor("simulation_head", namespace="doreisa")
            return
        except ValueError:
            time.sleep(0.1)


@ray.remote
def simple_worker(
    rank: int,
    position: tuple[int, ...],
    chunks_per_dim: tuple[int, ...],
    chunk_size: tuple[int, ...],
    nb_iterations: int,
    *,
    node_id: str | None = None,
) -> None:
    """Worker node sending chunks of data"""
    from doreisa.simulation_node import Client
    import numpy as np

    client = Client(_fake_node_id=node_id)

    array = (rank + 1) * np.ones(chunk_size, dtype=np.int32)

    for i in range(nb_iterations):
        client.add_chunk("array", position, chunks_per_dim, i * array, store_externally=False)
