import ray
from ray.util.dask import ray_dask_get, enable_dask_on_ray
import dask.array as da
import dask.dataframe as dd
import numpy as np
import pandas as pd


ray.init()
enable_dask_on_ray()

d_arr = da.from_array(np.random.randint(0, 1000, size=(256, 256)))

d_arr.mean().compute(scheduler=ray_dask_get)

df = dd.from_pandas(
    pd.DataFrame(np.random.randint(0, 100, size=(1024, 2)), columns=["age", "grade"]),
    npartitions=2,
)
df.groupby(["age"]).mean().compute()
