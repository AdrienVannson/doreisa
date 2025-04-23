import ray
from ray.util.dask import ray_dask_get


def doreisa_get(dsk: dict, keys, **kwargs):
    head_node = ray.get_actor("simulation_head", namespace="doreisa")  # noqa: F841

    return ray_dask_get(dsk, keys, **kwargs)
