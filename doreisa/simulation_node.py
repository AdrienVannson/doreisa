import logging
from typing import Callable

import cloudpickle
import numpy as np
import ray
import ray.actor
import zmq

from doreisa.in_transit_analytic_actor import SendChunkRequest


class Client:
    """
    Used by the MPI nodes to send data to the analytic cluster.

    The client is in charge of a several chunks of data. Each chunk is at a certain position in an
    array.
    """

    def __init__(self, *, _fake_node_id: str | None = None) -> None:
        """
        Args:
            _fake_node_id: The ID of the node. If None, the ID is taken from the Ray runtime context.
                This is useful for testing with several scheduling actors on a single machine.
        """
        if not ray.is_initialized():
            ray.init(address="auto", log_to_driver=False, logging_level=logging.ERROR)

        self.node_id = _fake_node_id or ray.get_runtime_context().get_node_id()

        self.head = ray.get_actor("simulation_head", namespace="doreisa")
        self.scheduling_actor: ray.actor.ActorHandle = ray.get(
            self.head.scheduling_actor.remote(self.node_id, is_fake_id=bool(_fake_node_id))
        )

        self.preprocessing_callbacks: dict[str, Callable] = ray.get(self.head.preprocessing_callbacks.remote())

    def add_chunk(
        self,
        array_name: str,
        chunk_position: tuple[int, ...],
        nb_chunks_per_dim: tuple[int, ...],
        nb_chunks_of_node: int,
        timestep: int,
        chunk: np.ndarray,
    ) -> None:
        """
        Make a chunk of data available to the analytic.

        Args:
            array_name: The name of the array.
            chunk_position: The position of the chunk in the array.
            nb_chunks_per_dim: The number of chunks per dimension.
            nb_chunks_of_node: The number of chunks sent by this node. The scheduling actor will
                inform the head actor when all the chunks are ready.
            chunk: The chunk of data.
        """
        chunk = self.preprocessing_callbacks[array_name](chunk)

        # Setting the owner allows keeping the reference when the simulation script terminates.
        ref = ray.put(chunk, _owner=self.scheduling_actor)

        future: ray.ObjectRef = self.scheduling_actor.add_chunk.options(enable_task_events=False).remote(
            array_name,
            timestep,
            chunk_position,
            chunk.dtype,
            nb_chunks_per_dim,
            nb_chunks_of_node,
            [ref],
            chunk.shape,
        )  # type: ignore

        # Wait until the data is processed before returning to the simulation
        ray.get(future)


class InTransitClient:
    def __init__(self, analytic_node_address: str):
        # Open a socket to communicate with the analytic node
        context = zmq.Context()
        self.socket = context.socket(zmq.REQ)
        self.socket.connect(f"tcp://{analytic_node_address}")

        # Get the preprocessing callbacks from the analytic node
        self.socket.send_pyobj("get_preprocessing_callbacks")
        self.preprocessing_callbacks: dict[str, Callable] = cloudpickle.loads(self.socket.recv_pyobj())

    def add_chunk(
        self,
        array_name: str,
        chunk_position: tuple[int, ...],
        nb_chunks_per_dim: tuple[int, ...],
        nb_chunks_of_analysis_node: int,
        timestep: int,
        chunk: np.ndarray,
    ) -> None:
        """
        nb_chunks_of_analysis_node: The number of chunks that the ANALYTIC node will
                receive for this array.
        """
        chunk = self.preprocessing_callbacks[array_name](chunk)

        # Send the data to the analytic node
        self.socket.send_pyobj(
            SendChunkRequest(
                array_name=array_name,
                chunk_position=chunk_position,
                nb_chunks_per_dim=nb_chunks_per_dim,
                nb_chunks_of_analysis_node=nb_chunks_of_analysis_node,
                timestep=timestep,
                chunk=chunk,
            )
        )

        # Wait for the response from the analytic node
        self.socket.recv()
