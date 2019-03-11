import os, time, datetime as dt
import multiprocessing as mp
import uuid as IdGenerator
import pandas as pd
import numpy as np

mile2km = 1.60934400
km2m = 1000
h2s = 3600
date_format = '%m/%d/%Y %I:%M:%S %p'

def filter(path, **kwargs):
    if 'pool_size' in kwargs.keys() and int(kwargs['pool_size']) > 1:
        pool = mp.Pool(int(kwargs['pool_size']))

        for file in os.listdir(path):
            if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file):
                pool.apply_async(filter_file, args=(path, file, kwargs))
        
        pool.close()
        pool.join()
    else:
        for file in os.listdir(path):
            if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file):
                filter_file(path, file, kwargs)

def filter_file(path, file, config):
    filename = os.path.join(path, file)
    df = pd.read_csv(filename, header=0)

    timezone = time.strftime("%z", time.gmtime())
    timezone = int(timezone.replace('+', '')) / 100 * 60 * 60
    df['pickup_unixdatetime'] = df['lpep_pickup_datetime'].map(lambda r: int(time.mktime(dt.datetime.strptime(r, date_format).timetuple())) + timezone)
    df['dropoff_unixdatetime'] = df['Lpep_dropoff_datetime'].map(lambda r: int(time.mktime(dt.datetime.strptime(r, date_format).timetuple())) + timezone)
    df['time_elapsed'] = df['dropoff_unixdatetime'] - df['pickup_unixdatetime']

    if 'max_lat' in config.keys() and 'min_lat' in config.keys():
        df = df[(df.Pickup_latitude <= config['max_lat']) & (df.Pickup_latitude >= config['min_lat']) & (df.Dropoff_latitude <= config['max_lat']) & (df.Dropoff_latitude >= config['min_lat'])]
    
    if 'max_lon' in config.keys() and 'min_lon' in config.keys():
        df = df[(df.Pickup_longitude <= config['max_lon']) & (df.Pickup_longitude >= config['min_lon']) & (df.Dropoff_longitude <= config['max_lon']) & (df.Dropoff_longitude >= config['min_lon'])]
    
    if 'max_timestamp' in config.keys() and 'min_timestamp' in config.keys():
        df = df[(df.dropoff_unixdatetime <= config['max_timestamp']) & (df.dropoff_unixdatetime >= config['min_timestamp']) & (df.pickup_unixdatetime <= config['max_timestamp']) & (df.pickup_unixdatetime >= config['min_timestamp'])]

    if 'max_speed' not in config.keys():
        config['max_speed'] = 100 * km2m / h2s
    cte = mile2km * km2m / config['max_speed']
    df = df[df['time_elapsed'] >= df['Trip_distance'] * cte]
    
    if 'min_speed' not in config.keys():
        config['min_speed'] = 5 * km2m / h2s
    cte = mile2km * km2m / config['min_speed']
    df = df[df['time_elapsed'] <= df['Trip_distance'] * cte]

    df = df[['lpep_pickup_datetime','Pickup_longitude','Pickup_latitude','Lpep_dropoff_datetime','Dropoff_longitude','Dropoff_latitude']]
    
    if 'split' in config.keys() and config['split']:
        for frame in np.array_split(df, int(config['split'])):
            filename = os.path.join(path, file[:-4] + '-' + IdGenerator.uuid4().hex + '.csv')
            frame.to_csv(filename, index=False)
        return
    
    if 'overwrite' in config.keys() and not config['overwrite']:
        filename = os.path.join(path, file[:-4] + '-' + IdGenerator.uuid4().hex + '.csv')
    df.to_csv(filename, index=False)