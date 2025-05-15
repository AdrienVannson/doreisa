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
    windows_size = {definition.name: definition.window_size for definition in arrays_description}

    head: Any = SimulationHead.options(**get_head_actor_options()).remote(head_arrays_description)

    # The array values needed for the analytics
    # Each list contains the arrays for several timesteps:
    #   - The list will be shorter than the window size during the first iterations
    #   - The list may be longer than the window size if the array are not produced in order
    all_arrays: dict[str, list[da.Array]] = {description.name: [] for description in arrays_description}

    for timestep in range(max_iterations):
        # Get new arrays
        while any(len(arrays) < windows_size[name] for name, arrays in all_arrays.items()):
            name: str
            timestep: int
            array: da.Array
            name, timestep, array = ray.get(head.get_next_array.remote())

            all_arrays[name].append(array)

        # Remove the most recent arrays if we received them in advanced
        all_arrays_cropped = {name: arrays[: windows_size[name]] for name, arrays in all_arrays.items()}

        # TODO check that the arrays were indeed produced at the same timestep
        # TODO using the last timestep might be wrong

        simulation_callback(**all_arrays_cropped, timestep=timestep)

        # Remove the oldest arrays
        for name, arrays in all_arrays.items():
            if len(arrays) >= windows_size[name]:
                arrays.pop(0)
