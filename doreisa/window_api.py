import gc
from dataclasses import dataclass
from typing import Any, Callable

import dask.array as da
import ray

from doreisa.head_node import ArrayDefinition as HeadArrayDefinition
from doreisa.head_node import SimulationHead, get_head_actor_options


@dataclass
class ArrayDefinition:
    name: str
    window_size: int
    preprocess: Callable = lambda x: x


def run_simulation(
    simulation_callback: Callable, arrays_description: list[ArrayDefinition], *, max_iterations=1000_000_000
) -> None:
    # Convert the definitions to the type expected by the head node
    head_arrays_description = [
        HeadArrayDefinition(name=definition.name, preprocess=definition.preprocess) for definition in arrays_description
    ]

    # Limit the advance the simulation can have over the analytics
    max_pending_arrays = 2 * len(arrays_description)

    head: Any = SimulationHead.options(**get_head_actor_options()).remote(head_arrays_description, max_pending_arrays)

    arrays_by_iteration: dict[int, dict[str, da.Array]] = {}

    for iteration in range(max_iterations):
        # Get new arrays
        while len(arrays_by_iteration.get(iteration, {})) < len(arrays_description):
            name: str
            timestep: int
            array: da.Array
            name, timestep, array = ray.get(head.get_next_array.remote())

            if timestep not in arrays_by_iteration:
                arrays_by_iteration[timestep] = {}

            assert name not in arrays_by_iteration[timestep]
            arrays_by_iteration[timestep][name] = array

        # Compute the arrays to pass to the callback
        all_arrays: dict[str, list[da.Array]] = {}

        for description in arrays_description:
            all_arrays[description.name] = [
                arrays_by_iteration[timestep][description.name]
                for timestep in range(max(iteration - description.window_size + 1, 0), iteration + 1)
            ]

        simulation_callback(**all_arrays, timestep=timestep)

        del all_arrays

        # Remove the oldest arrays
        for description in arrays_description:
            older_timestep = iteration - description.window_size + 1
            if older_timestep >= 0:
                del arrays_by_iteration[older_timestep][description.name]

                if not arrays_by_iteration[older_timestep]:
                    del arrays_by_iteration[older_timestep]

        # Free the memory used by the arrays now. Since an ObjectRef is a small object,
        # Python may otherwise choose to keep it in memory for some time, preventing the
        # actual data to be freed.
        gc.collect()
