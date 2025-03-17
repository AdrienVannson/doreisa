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

        self.preprocessing_callback: Callable = ray.get(self.head.get_preprocessing_callback.remote())

    def simulation_step(self, *chunks: np.ndarray) -> None:
        chunks: dict[tuple[str, tuple[int, ...]], np.array] = self.preprocessing_callback(chunks, self.rank, self.timestep)

        for chunk_info, chunk in chunks.items():
            chunk_ref = ray.put(chunk)

            future = self.head.add_chunk.remote(chunk_info[0], self.timestep, chunk_info[1], [chunk_ref])

            # Wait until the data is processed before returning to the simulation
            # TODO the synchronization is not that good
            ray.get(future)

        self.timestep += 1
