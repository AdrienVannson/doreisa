import ray
import pytest
import subprocess


@pytest.fixture
def ray_cluster():
    """Start a Ray cluster for this test"""
    subprocess.run(["ray", "start", "--head"], check=True)

    yield

    if ray.is_initialized():
        ray.shutdown()

    subprocess.run(["ray", "stop"], check=True)


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

    client = Client(_node_id=node_id)

    array = (rank + 1) * np.ones(chunk_size, dtype=np.int32)

    for i in range(nb_iterations):
        client.add_chunk("array", position, chunks_per_dim, i * array, store_externally=False)
