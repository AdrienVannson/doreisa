import numpy as np
import ray


class Client:
    """
    Used by the MPI nodes to send data to the analytic cluster.
    """

    def __init__(self, rank: int) -> None:
        self.rank = rank
        print(f"New client created (rank={rank})")

        self.head = ray.get_actor("simulation_head", namespace="doreisa")
        ray.get(self.head.set_worker_ready.remote(worker_id=self.rank))

    def simulation_step(self, temperatures: np.ndarray) -> None:
        ref = ray.put(temperatures)

        future = self.head.simulation_step.remote(worker_id=self.rank, grid=[ref])

        # Wait until the data is processed before returning to the simulation
        ray.get(future)
