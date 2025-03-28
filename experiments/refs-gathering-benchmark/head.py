import os

os.environ["RAY_worker_register_timeout_seconds"] = "3600"

import asyncio
import time
import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

ray.init()

start_time = None


@ray.remote
class Actor:
    def __init__(self):
        self.iteration = 0
        self.counter = 0
        self.event_ready = asyncio.Event()

    async def add_chunk(self, chunk, nb_workers):
        self.counter += 1

        if self.counter == nb_workers:
            self.counter = 0
            self.event_ready.set()
            self.event_ready.clear()

            self.iteration += 1

            if self.iteration == 50:
                global start_time
                start_time = time.time()
            elif self.iteration == 250:
                with open("measurements.txt", "a") as f:
                    f.write(f"{nb_workers} {len(chunk)}: {time.time() - start_time}\n")

        else:
            await self.event_ready.wait()


head = Actor.options(
    name="simulation_head",
    namespace="doreisa",
    # Schedule the actor on this node
    scheduling_strategy=NodeAffinitySchedulingStrategy(
        node_id=ray.get_runtime_context().get_node_id(),
        soft=False,
    ),
).remote()

time.sleep(3600)
