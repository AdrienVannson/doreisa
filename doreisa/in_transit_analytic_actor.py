from collections.abc import Callable
from dataclasses import dataclass

import cloudpickle
import numpy as np
import ray
import ray.actor
import zmq.asyncio


@dataclass
class SendChunkRequest:
    """
    Represents a request to send a chunk of data to the analytic node.
    """

    array_name: str
    chunk_position: tuple[int, ...]
    nb_chunks_per_dim: tuple[int, ...]
    nb_chunks_of_analysis_node: int
    timestep: int
    chunk: np.ndarray


@ray.remote
class InTransitAnalyticActor:
    """
    Actor that runs a ZMQ server to receive data from the simulation nodes.
    """

    def __init__(self, zmq_address: str, *, _fake_node_id: str | None = None) -> None:
        self.node_id = _fake_node_id or ray.get_runtime_context().get_node_id()

        # Get the head actor and the scheduling actor
        self.head = ray.get_actor("simulation_head", namespace="doreisa")
        self.scheduling_actor: ray.actor.ActorHandle = ray.get(
            self.head.scheduling_actor.remote(self.node_id, is_fake_id=bool(_fake_node_id))
        )

        self.preprocessing_callbacks: dict[str, Callable] = ray.get(self.head.preprocessing_callbacks.remote())

        context = zmq.asyncio.Context()
        self.socket = context.socket(zmq.REP)
        self.socket.bind(zmq_address)

    async def run_zmq_server(self):
        while True:
            message = await self.socket.recv_pyobj()

            if message == "get_preprocessing_callbacks":
                # Send the preprocessing callbacks to the client
                # Cloudpickle is needed since pickle fails to serialize the callbacks
                await self.socket.send_pyobj(cloudpickle.dumps(self.preprocessing_callbacks))
                continue

            assert isinstance(message, SendChunkRequest)

            await self.scheduling_actor.add_chunk.remote(
                array_name=message.array_name,
                timestep=message.timestep,
                chunk_position=message.chunk_position,
                dtype=message.chunk.dtype,
                nb_chunks_per_dim=message.nb_chunks_per_dim,
                nb_chunks_of_node=message.nb_chunks_of_analysis_node,
                chunk=[ray.put(message.chunk)],
                chunk_shape=message.chunk.shape,
            )
