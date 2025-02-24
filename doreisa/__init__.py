import ray
import ray.util.dask

def init():
    ray.init()
    ray.util.dask.enable_dask_on_ray()