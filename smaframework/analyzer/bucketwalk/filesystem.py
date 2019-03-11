import os
import matplotlib
import pandas as pd
from geopy import Point
import uuid as IdGenerator
from geopy import distance
import multiprocessing as mp
from math import sin, cos, atan2, floor, sqrt, radians
import smaframework.tool.paralel as Paralel
from functools import partial
import itertools, json, sys

def histogram(path, layers, show=True, max_x=None, save_log=True, **kwargs):
    if isinstance(layers, str):
        layers = [layers]

    for layer in layers:
        if 'pool_size' in kwargs.keys() and int(kwargs['pool_size']) > 1:
            pool = mp.Pool(int(kwargs['pool_size']))
            result = pool.map(load_csv, [(path, file, layer) for file in os.listdir(path) if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file)])
            pool.close()
            pool.join()
        else:
            result = []
            for file in os.listdir(path):
                if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file):
                    result.append(load_csv((path, file, layer)))

        if len(result) == 0:
            print('Layer %s empty!' % layer)
            continue

        frame = pd.concat(list(result))

        if len(frame) == 0:
            print('Layer %s empty!' % layer)
            continue

        frame = frame.groupby(['lat_bucket','lon_bucket','timestamp_bucket']).size()
        maximum = frame.max()
        frame = frame.map(lambda a: a / maximum)

        if pd.__version__ >= '0.17.0':
            frame.sort_values(ascending=False, inplace=True)
        else:
            frame.sort(ascending=False, inplace=True)

        if len(frame) == 0:
            print('Layer %s empty!' % layer)
            continue

        if save_log:
            frame.to_csv('data/results/%s-bucket-histogram.log' % layer)
        
        frame.index = [i for i in range(0, len(frame))]
        plot = frame.plot(kind='line', label=layer + (' (max: %d)' % maximum))
    
        if max_x:
            plot.axis([0,max_x,0,maximum+1])
        # plot.set_yscale('log', nonposy='clip')
        plot.legend()
    
    if show:
        matplotlib.pyplot.show(block=True)
    else:
        fig = plot.get_figure()
        fig.savefig('data/results/bucket-histogram.png')

    # df1 = pd.read_csv(
    #     'data/twitter-bucket-histogram.log', 
    #     header=None, 
    #     skiprows=1, 
    #     low_memory=False, 
    #     memory_map=True, 
    #     names=['i','j','k','c']
    #     )['c']

    # paddf = pd.DataFrame([0 for i in range(200610, 2000000)])
    # df1 = pd.concat([df1, paddf])

    # df1.index = [i for i in range(0,len(df1))]
    # plot = df1.plot(kind='line', label='twitter: (200610 used buckets)', color='r')
    # # maximum = df1.max()
    # # print(maximum)
    # plot.axis([0,500000,0,1200])
    # # matplotlib.pyplot.yscale('log')

    # plot.text(0, 420, '(0, 420)', color='r')
    # # plot.plot(0, 420, 'ro')
    # # plot.text(350000, 800, "twitter: \nyellow_taxis (1996165 used buckets)")
    # # plot.plot(187, 10, 'ro')
    # plot.legend()

    # df2 = pd.read_csv(
    #     'data/yellow_taxis-bucket-histogram.log', 
    #     header=None, 
    #     skiprows=1, 
    #     low_memory=False, 
    #     memory_map=True, 
    #     names=['i','j','k','c']
    #     )['c']
    # df2.index = [i for i in range(0,len(df2))]
    # plot = df2.plot(kind='line', label='yellow_taxis: (1996165 used buckets)', color='b')
    # # maximum = df2.max()
    # # print(maximum)
    # plot.axis([0,500000,0,1200])
    # # matplotlib.pyplot.yscale('log')

    # plot.text(0, 1130, '(0, 1130)', color='b')
    # # plot.plot(0, 1130, 'bo')
    # # plot.text(500686, 10, '10', color='b')
    # # plot.plot(500686, 10, 'bo')
    # plot.legend()
    
    # matplotlib.pyplot.show(block=True)

    # # main('data/buckets/', 'twitter', 16, False, 4)
    # # main('data/buckets/', 'yellow_taxis', 16, False, 4)

def index(path, hashing_distance, origin, **kwargs):
    multiprocess = 'pool_size' in kwargs.keys() and int(kwargs['pool_size']) > 1
    if multiprocess:
        pool_size = int(kwargs['pool_size'])
        pool = mp.Pool(pool_size)
        result = pool.map(load_csv, [(path, file) for file in os.listdir(path) if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file)])
        pool.close()
        pool.join()
    else:
        result = []
        for file in os.listdir(path):
            if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file):
                result.append(load_csv((path, file)))

    frame = pd.concat(list(result))

    if 'verbose' in kwargs.keys() and kwargs['verbose']:
        print('Data loaded...', flush=True)

    frame = Paralel.map(frame, hash_df, hashing_distance, origin)

    if 'verbose' in kwargs.keys() and kwargs['verbose']:
        print('Data hashed...', flush=True)

    buckets_location = path + 'bucketwalk-index/'
    if not os.path.exists(buckets_location):
        os.makedirs(buckets_location)

    frame = frame.groupby(by=[key + '_bucket' for key in sorted(hashing_distance.keys())])
    for name, g in frame:
        format_str = '-%d' * len(hashing_distance.keys())
        format_str = format_str[1:]
        format_str = '%s'+ format_str +'.csv'

        format_params = [buckets_location]
        format_params.extend(name)
        format_params = tuple(format_params)

        g.to_csv(format_str % format_params, mode='a')
        
    if 'verbose' in kwargs.keys() and kwargs['verbose']:
        print('Buckets indexed...', flush=True)

    json.dump({
        "hashing_distance": hashing_distance,
        "origin": origin,
        "path": buckets_location
        }, open(buckets_location + 'metadata.json', 'w+'))

def load_csv(args):
    path, file = args
    return pd.read_csv(
        os.path.join(path, file), 
        header=0, 
        low_memory=False, 
        memory_map=True, 
        index_col='id'
        )

def hash_sample(hashing_distance, origin, point):
    return int((point - origin) / hashing_distance)

def hash_df(params):
    df, args, kwargs = (params)
    hashing_distance, origin = args
    for dimension in sorted(hashing_distance.keys()):
        df['%s_bucket' %  dimension] = df[dimension].map(partial(hash_sample, hashing_distance[dimension], origin[dimension]))

    return df

def closest(index_path, point, dist, radius=1, **kwargs):
    index = json.load(open(index_path, 'r'))
    key = {}
    for c in point.keys():
        key[c] = hash_sample(index['hashing_distance'][c], index['origin'][c], point[c])

    cube = get_cube(index, key, radius)

    if 'layers' in kwargs.keys():
        cube = cube[cube['layer'].isin(kwargs['layers'])]

    cube['distance'] = cube[list(sorted(index['origin'].keys()))].apply(dist, axis=1)
    minimum = cube['distance'].min()

    return cube[cube['distance'] == minimum]

def get_cube(index, key, radius):
    ranges = list(map(lambda i: list(range(key[i] - radius, key[i] + radius + 1)), [i for i in sorted(key.keys())]))

    keys = list(itertools.product(*ranges))
    keys = list(map(lambda key: '-'.join([str(k) for k in key]), keys))

    result = []
    for k in keys:
        filename = index['path'] + k + '.csv'
        if not os.path.isfile(filename):
            continue
        result.append(pd.read_csv(filename))

    if len(result) == 0:
        return pd.DataFrame()
    return pd.concat(result, axis=0)