from haversine import haversine
import os, time, datetime as dt
import multiprocessing as mp
import uuid as IdGenerator
import pandas as pd
import numpy as np

# constant to convert miles to km
mile2km = 1.60934400

# constant to conver km to m
km2m = 1000

# constant to convert hours to seconds
h2s = 3600

# date format used in the original file
date_format = '%Y-%m-%d %H:%M:%S'

'''
 * @param path: the path of the directory containing the data file (also used to save the output file)
 * @param file: the name of the input data file
 * @param **kwargs: allows the specification of custom values for fields, such as: 
 *    min_lat
 *    max_lat
 *    min_lon
 *    max_lon
 *    min_timestamp (unix timestamp)
 *    max_timestamp (unix timestamp)
 *    max_speed  (km/h)
 *    min_speed  (km/h)
'''
def filter(path, file, **kwargs):
    filename = os.path.join(path, file)
    df = pd.read_csv(filename, header=0)

    timezone = time.strftime("%z", time.gmtime())
    timezone = int(timezone.replace('+', '')) / 100 * 60 * 60
    df['pickup_unixdatetime'] = df['tpep_pickup_datetime'].map(lambda r: int(time.mktime(dt.datetime.strptime(r, date_format).timetuple())) + timezone)
    df['dropoff_unixdatetime'] = df['tpep_dropoff_datetime'].map(lambda r: int(time.mktime(dt.datetime.strptime(r, date_format).timetuple())) + timezone)
    df['time_elapsed'] = df['dropoff_unixdatetime'] - df['pickup_unixdatetime']

    config = {
        'min_lat': 40.632,
        'max_lat': 40.849,
        'min_lon': -74.060,
        'max_lon': -73.762,
        'min_timestamp': 1464739200,
        'max_timestamp': 1467331199,
        'max_speed': 100 * km2m / h2s,
        'min_speed': 5 * km2m / h2s,
        }
    config.update(kwargs)

    df = df[(df['pickup_latitude'] <= config['max_lat']) & (df['pickup_latitude'] >= config['min_lat']) & (df['dropoff_latitude'] <= config['max_lat']) & (df['dropoff_latitude'] >= config['min_lat'])]
    df = df[(df['pickup_longitude'] <= config['max_lon']) & (df['pickup_longitude'] >= config['min_lon']) & (df['dropoff_longitude'] <= config['max_lon']) & (df['dropoff_longitude'] >= config['min_lon'])]    
    df = df[(df['dropoff_unixdatetime'] <= config['max_timestamp']) & (df['dropoff_unixdatetime'] >= config['min_timestamp']) & (df['pickup_unixdatetime'] <= config['max_timestamp']) & (df['pickup_unixdatetime'] >= config['min_timestamp'])]

    cte = km2m / config['max_speed']
    df = df[df['time_elapsed'] >= df['trip_distance'] * cte]
    
    cte = mile2km * km2m / config['min_speed']
    df = df[df['time_elapsed'] <= df['trip_distance'] * cte]

    df = df[['tpep_pickup_datetime','pickup_latitude','pickup_longitude','tpep_dropoff_datetime','dropoff_latitude','dropoff_longitude']]
    
    if 'split' in config.keys() and config['split']:
        for frame in np.array_split(df, int(config['split'])):
            filename = os.path.join(path, file[:-4] + '-' + IdGenerator.uuid4().hex + '.csv')
            frame.to_csv(filename, index=False)
        return
    
    if 'overwrite' in config.keys() and not config['overwrite']:
        filename = os.path.join(path, file[:-4] + '-' + IdGenerator.uuid4().hex + '.csv')
    df.to_csv(filename, index=False)

    return filename