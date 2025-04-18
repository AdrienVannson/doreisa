import numpy as np
import ray
from typing import Callable

import ray.actor


class Client:
    """
    Used by the MPI nodes to send data to the analytic cluster.

    The client is in charge of a several chunks of data. Each chunk is at a certain position in an
    array.
    """

    def __init__(self, *, _node_id: str | None = None) -> None:
        """
        Args:
            _node_id: The ID of the node. If None, the ID is taken from the Ray runtime context.
                This is useful for testing with several scheduling actors on a single machine.
        """
        if not ray.is_initialized():
            ray.init(address="auto")

        self.node_id = _node_id or ray.get_runtime_context().get_node_id()

        self.head = ray.get_actor("simulation_head", namespace="doreisa")
        self.scheduling_actor: ray.actor.ActorHandle = ray.get(self.head.scheduling_actor.remote(self.node_id, is_fake_id=bool(_node_id)))

        self.preprocessing_callbacks: dict[str, Callable] = ray.get(self.head.preprocessing_callbacks.remote())

    def add_chunk(
        self,
        array_name: str,
        chunk_position: tuple[int, ...],
        nb_chunks_per_dim: tuple[int, ...],
        chunk: np.ndarray,
        store_externally: bool = False,
    ) -> None:
        chunk = self.preprocessing_callbacks[array_name](chunk)

        # TODO add a test to check that _owner allows the script to terminate without loosing the ref
        ref = ray.put(chunk, _owner=self.scheduling_actor)

        future = self.head.add_chunk.remote(array_name, chunk_position, nb_chunks_per_dim, [ref], chunk.shape)

        # Wait until the data is processed before returning to the simulation
        ray.get(future)
