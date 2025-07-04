# Analytics

## Simple example

```python
from doreisa.head_node import init
from doreisa.window_api import ArrayDefinition, run_simulation

init()

def simulation_callback(array: da.Array, timestep: int):
    x = array.sum().compute()
    print("Sum:", x)

run_simulation(
    simulation_callback,
    [ArrayDefinition("array")],
)
```

## Several arrays

```python
from doreisa.head_node import init
from doreisa.window_api import ArrayDefinition, run_simulation

init()

def simulation_callback(a: da.Array, b: da.Array, timestep: int):
    r = (a - b).mean().compute()

run_simulation(
    simulation_callback,
    [ArrayDefinition("a"), ArrayDefinition("b")]
)
```

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

## Dask persist

Dask's `persist` is supported:

```python
from doreisa.head_node import init
from doreisa.window_api import ArrayDefinition, run_simulation

init()

def simulation_callback(array: da.Array, timestep: int):
    x = array.sum().persist()

    # x is still a Dask array, but the sum is being computed in the background
    assert isinstance(x, da.Array)

    x_final = x.compute()
    assert x_final == 10 * timestep

run_simulation(
    simulation_callback,
    [ArrayDefinition("array")],
    max_iterations=NB_ITERATIONS,
)
```

## Preprocessing callbacks

A preprocessing callback is a function that is applied on each chunk of data. The function is executed locally, on the machine where the data is produced as soon as it is available.

```python
from doreisa.head_node import init
from doreisa.window_api import ArrayDefinition, run_simulation

init()

def simulation_callback(array: da.Array, timestep: int):
    ...

run_simulation(
    simulation_callback,
    [ArrayDefinition("array", preprocess=lambda chunk: 10 * chunk)],
)
```