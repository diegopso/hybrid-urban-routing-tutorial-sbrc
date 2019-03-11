import os
import multiprocessing as mp
import numpy as np
import pandas as pd
import time
import datetime as dt
import uuid as IdGenerator

def extract_file(args):
    file, source, dest, nodes, layer, config = args
    filename = os.path.join(source, file)

    df = pd.read_csv(filename, header=0)

    if 'id' not in df.columns:
        df['id'] = df.index.map(lambda r: int(r))
        
    df = df[list(set().union(*nodes))]

    cdf = pd.DataFrame()
    for node in nodes:
        tdf = df[node]
        tdf.columns = ['uid', 'timestamp', 'lat', 'lon']
        cdf = pd.concat([cdf, tdf])

    df = cdf
    df['layer'] = layer

    if 'datetime_format' in config.keys() and config['datetime_format'] != '%u':
        timezone = time.strftime("%z", time.gmtime())
        timezone = int(timezone.replace('+', '')) / 100 * 60 * 60
        df['timestamp'] = df['timestamp'].map(lambda r: int(time.mktime(dt.datetime.strptime(r, config['datetime_format']).timetuple())) + timezone)
    else:
        df['timestamp'] = df['timestamp'].map(lambda r: int(r))

    if 'max_lat' in config.keys() and 'min_lat' in config.keys():
        df = df[(df.lat <= config['max_lat']) & (df.lat >= config['min_lat'])]
    
    if 'max_lon' in config.keys() and 'min_lon' in config.keys():
        df = df[(df.lon <= config['max_lon']) & (df.lon >= config['min_lon'])]
    
    if 'max_timestamp' in config.keys() and 'min_timestamp' in config.keys():
        df = df[(df.timestamp <= config['max_timestamp']) & (df.timestamp >= config['min_timestamp'])]

    file_id = IdGenerator.uuid4().hex

    pd.options.mode.chained_assignment = None
    df['uid'] = df['uid'].map(lambda x: str(x) + file_id)
    pd.options.mode.chained_assignment = 'warn'

    if not os.path.exists(dest):
        os.makedirs(dest)
    df.to_csv(os.path.join(dest, file), index=False)

def extract(source, dest, layer, nodes, **kwargs):
    multiprocess = 'pool_size' in kwargs.keys() and int(kwargs['pool_size']) > 1
    filelist = os.listdir(source)

    if multiprocess:
        pool_size = int(kwargs['pool_size'])
        pool = mp.Pool(pool_size)
        pool.map(extract_file, [(file, source, dest, nodes, layer, kwargs) for file in filelist if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file)])
        pool.close()
        pool.join()
    else:
        for file in filelist:
            if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file):
                extract_file((file, source, dest, nodes, layer, kwargs))