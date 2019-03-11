# import dask.dataframe as dd
import multiprocessing as mp
import numpy as np
import pandas as pd

# def prepare(df, **kwargs):
#     if 'pool_size' in kwargs.keys():
#         kwargs['npartitions'] = kwargs['pool_size']
#         del kwargs['pool_size']
#     elif 'npartitions' not in kwargs.keys() and 'chunksize' not in kwargs.keys():
#         kwargs['npartitions'] = 1

#     return dd.from_pandas(df, **kwargs)

def map(df, callback, *args, **kwargs):
    if 'pool_size' not in kwargs.keys():
        kwargs['pool_size'] = 1

    chunksize = len(df) / kwargs['pool_size']
    groups = df.groupby(np.arange(len(df)) // chunksize)
    with mp.Pool() as pool:
        result = pool.map(callback, [(df, args, kwargs) for g, df in groups])
        return pd.concat(result, axis=0)