from tests.utils import ray_cluster  # noqa: F401


NB_ITERATIONS = 10

def head() -> None:
    """The head node checks that the values are correct"""
    import asyncio
    import doreisa.head_node as doreisa
    import dask.array as da

    doreisa.init()

    def simulation_callback(array: list[da.Array], timestep: int):
        x = array[0].sum().compute()

        if x != 10 * timestep:
            exit(1)

        with open("test.txt", "a") as f:
            f.write(f"timestep {timestep}: {x}\n")

        if timestep == NB_ITERATIONS - 1:
            exit(0)

    asyncio.run(doreisa.start(simulation_callback, [
        doreisa.DaskArrayInfo("array", window_size=1),
    ]))


def worker(rank: int) -> None:
    """Worker nodes send chunks of data"""
    from doreisa.simulation_node import Client
    import numpy as np

    client = Client(rank)

    array = np.array(rank + 1, dtype=np.int32)

    for i in range(NB_ITERATIONS):
        client.add_chunk("array", (rank // 2, rank % 2), (2, 2), i * array, store_externally=False)


def test_doreisa(ray_cluster) -> None:  # noqa: F811
    import multiprocessing as mp
    import time

    head_process = mp.Process(target=head, daemon=True)
    head_process.start()

    time.sleep(5)
    
    worker_processes = []
    for rank in range(4):
        worker_process = mp.Process(target=worker, args=(rank,), daemon=True)
        worker_process.start()
        worker_processes.append(worker_process)

    head_process.join()
    assert head_process.exitcode == 0
