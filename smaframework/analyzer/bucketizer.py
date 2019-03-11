import os
import matplotlib
import pandas as pd
from geopy import Point
import uuid as IdGenerator
from geopy import distance
import multiprocessing as mp
from math import sin, cos, atan2, floor, sqrt, radians

# @deprecated, use smaframework.analyzer.bucketwalk.filesystem

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

def index(path, distance_precision, temporal_precision, layer, **kwargs):
    multiprocess = 'pool_size' in kwargs.keys() and int(kwargs['pool_size']) > 1
    if multiprocess:
        pool_size = int(kwargs['pool_size'])
        pool = mp.Pool(pool_size)
        result = pool.map(load_csv, [(path, file, layer) for file in os.listdir(path) if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file)])
        pool.close()
        pool.join()
    else:
        result = []
        for file in os.listdir(path):
            if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file):
                result.append(load_csv((path, file, layer)))

    frame = pd.concat(list(result))
    if 'verbose' in kwargs.keys() and kwargs['verbose']:
        print('Data loaded...', flush=True)

    # split layers
    layer1 = frame[frame.layer == layer].groupby(['lat_bucket','lon_bucket','timestamp_bucket'])
    if 'verbose' in kwargs.keys() and kwargs['verbose']:
        print('Layers splited...', flush=True)

    buckets_location = 'data/buckets/index/'
    if not os.path.exists(buckets_location):
        os.makedirs(buckets_location)

    for name, g in layer1:
        g.to_csv('data/buckets/index/%s-%d-%d-%d.csv' % (layer.replace('-', '_'), name[0], name[1], name[2]))
        
    if 'verbose' in kwargs.keys() and kwargs['verbose']:
        print('Buckets indexed...', flush=True)

def load_csv(args):
    path, file, layer = args
    df = pd.read_csv(
        os.path.join(path, file), 
        header=0, 
        low_memory=False, 
        memory_map=True, 
        index_col='id'
        )

    return df[(df.layer == layer)]

def bucketize(path, origin, distance_precision, time_precision, **kwargs):
    multiprocess = 'pool_size' in kwargs.keys() and int(kwargs['pool_size']) > 1
    if multiprocess:
        pool_size = int(kwargs['pool_size'])
        pool = mp.Pool(pool_size)

    filelist = os.listdir(path)
    for file in filelist:
        if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file):
            if multiprocess:
                pool.apply_async(bucketize_file, args=(path, file, origin, distance_precision, time_precision, kwargs))
            else:
                bucketize_file(path, file, origin, distance_precision, time_precision, kwargs)

    if multiprocess:
        pool.close()
        pool.join()

def bucketize_file(path, file, origin, distance_precision, time_precision, config):
    filename = os.path.join(path, file)
    df = pd.read_csv(filename, header=0)

    tdf = df[['lat', 'lon', 'timestamp']]
    tdf.columns = ['lat_bucket', 'lon_bucket', 'timestamp_bucket']
    df = pd.concat([df, tdf], axis=1)

    fileid = IdGenerator.uuid4().hex
    df['uid'] = df['uid'].map(lambda x: x + fileid)

    df['lat_bucket'] = df['lat_bucket'].map(lambda x: floor(lat(origin, x) / distance_precision))
    df['lon_bucket'] = df['lon_bucket'].map(lambda x: floor(lon(origin, x) / distance_precision))
    df['timestamp_bucket'] = df['timestamp_bucket'].map(lambda x: floor((x - origin[2]) / time_precision))

    buckets_location = 'data/buckets/'
    if not os.path.exists(buckets_location):
        os.makedirs(buckets_location)

    df.to_csv(buckets_location + file, index=False)

def lat(origin, l):
    p1 = Point("%f %f" % (origin[0], origin[1]))
    p2 = Point("%f %f" % (l, origin[1]))
    return distance.distance(p1, p2).meters

def lon(origin, l):
    p1 = Point("%f %f" % (origin[0], origin[1]))
    p2 = Point("%f %f" % (origin[0], l))
    return distance.distance(p1, p2).meters