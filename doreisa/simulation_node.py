import numpy as np
import ray
from dataclasses import dataclass


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

    def __init__(self) -> None:
        self.head = ray.get_actor("simulation_head", namespace="doreisa")

        self.timestep = 0

        # The chunks that the client is in charge of updating
        self.chunks: list[_Chunk] = []

    def create_array(self, name: str, nb_chunks_per_dim: tuple[int, ...], chunk_position: tuple[int, ...]) -> None:
        self.head.create_array.remote(name, nb_chunks_per_dim)

        self.chunks.append(_Chunk(name, chunk_position))

    def simulation_step(self, chunks: list[np.ndarray]) -> None:
        assert len(chunks) == len(self.chunks)

        for chunk, chunk_info in zip(chunks, self.chunks):
            ref = ray.put(chunk)

            future = self.head.add_chunk.remote(chunk_info.array_name, self.timestep, chunk_info.chunk_position, [ref])

            # Wait until the data is processed before returning to the simulation
            # TODO the synchronization is not that good
            ray.get(future)

        self.timestep += 1
