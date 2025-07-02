# Analytics

The analysis of the data produced by the simulation is done by defining a callback function.

## Sliding window

If the analysis requires access to several iterations (for example, to compute time derivative), it is possible to use the `window_size` parameter.

```python
from doreisa.head_node import init
from doreisa.window_api import ArrayDefinition, run_simulation

init()

def simulation_callback(array: list[da.Array], timestep: int):
    if len(arrays) < 2:  # For the first iteration
        return

    current_array = array[1]
    previous_array = array[0]

    ...

run_simulation(
    simulation_callback,
    [
        ArrayDefinition("array", window_size=2),  # Enable sliding window
    ],
)
```