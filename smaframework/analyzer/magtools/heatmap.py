import os, json
import multiprocessing as mp
import pandas as pd
import numpy as np

def _heatmap_file(args):
    filename, layer = args
    df = pd.read_csv(filename, header=0)
    df = df[df['layer'] == layer]
    return df.apply(lambda r: '{location: new google.maps.LatLng(%f, %f)},' % (r['lat'], r['lon']), axis=1)

def _heatmap_bucket(args):
    filename, layer = args
    df = pd.read_csv(filename, header=0)
    df = df[df['layer'] == layer]
    lat = df['lat'].mean()
    lon = df['lon'].mean()
    weight = len(df)
    return '{location: new google.maps.LatLng(%f, %f), weight: %f},' % (lat, lon, weight)

def layer(layer, key, path, **kwargs):
    if 'buckets' in kwargs.keys() and kwargs['buckets']:
        f = _heatmap_bucket
    else:
        f = _heatmap_file

    multiprocess = 'pool_size' in kwargs.keys() and int(kwargs['pool_size']) > 1
    if multiprocess:
        pool_size = int(kwargs['pool_size'])
        pool = mp.Pool(pool_size)
        results = pool.map(f, [(os.path.join(path, file), layer) for file in os.listdir(path) if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file)])
        pool.close()
        pool.join()
    else:
        results = []
        for file in os.listdir(path):
            if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file):
                results.append(f((os.path.join(path, file), layer)))

    if 'buckets' not in kwargs.keys() or not kwargs['buckets']:
        frame = pd.concat(results)
        if len(frame) == 0:
            return None
        results = frame[0].tolist()
    results = '\n'.join(results)
    
    with open('templates/google-heatmap.html', 'r') as file:
        template = file.read()

    template = template.replace('<?=LIST?>', results).replace('<?=KEY?>', key)

    if 'filename' in kwargs.keys():
        filename = kwargs['filename']
    else:
        filename = 'heatmap-%s.html' % layer

    with open('data/results/' + filename, 'w+') as file:
        file.write(template)