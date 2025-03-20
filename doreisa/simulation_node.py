import numpy as np
import ray
from dataclasses import dataclass
from typing import Callable


@dataclass
class _Chunk:
    array_name: str
    chunk_position: tuple[int, ...]


class Client:
    """
    Used by the MPI nodes to send data to the analytic cluster.

    The client is in charge of a several chunks of data. Each chunk is at a certain position in an
    array.
    """

    def __init__(self, rank: int) -> None:
        self.head = ray.get_actor("simulation_head", namespace="doreisa")

        self.rank = rank
        self.timestep = 0

        self.preprocessing_callbacks: dict[str, Callable] = ray.get(self.head.preprocessing_callbacks.remote())

    def add_chunk(self, array_name: str, chunk_position: tuple[int, ...], chunk: np.ndarray, store_externally: bool = False) -> None:
        chunk = self.preprocessing_callbacks[array_name](chunk)

        future = self.head.add_chunk.remote(array_name, self.timestep, chunk_position, [ray.put(chunk)], chunk.shape)

        # Wait until the data is processed before returning to the simulation
        ray.get(future)

    def next_timestep(self) -> None:
        self.timestep += 1
