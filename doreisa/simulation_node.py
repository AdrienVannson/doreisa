import numpy as np
import ray


class Client:
    """
    Used by the MPI nodes to send data to the analytic cluster.

    The client is in charge of a single chunk of data. It will send it at each timestep.
    """

    def __init__(self, array_name: str, chunk_position: tuple[int, ...]) -> None:
        self.head = ray.get_actor("simulation_head", namespace="doreisa")

        self.timestep = 0
        self.array_name = array_name
        self.chunk_position = chunk_position

    def create_array(self, name: str, nb_chunks_per_dim: tuple[int, ...]) -> None:
        self.head.create_array.remote(name, nb_chunks_per_dim)

    def simulation_step(self, temperatures: np.ndarray) -> None:
        ref = ray.put(temperatures)

        future = self.head.add_chunk.remote(self.array_name, self.timestep, self.chunk_position, [ref])
        self.timestep += 1

        # Wait until the data is processed before returning to the simulation
        ray.get(future)
