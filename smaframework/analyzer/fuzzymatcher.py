import os, json, math, re, gc, base64
import multiprocessing as mp
import pandas as pd
import numpy as np
from geopy import distance
from geopy import Point
import uuid as IdGenerator
from random import randint
import sklearn
from sklearn.cluster import DBSCAN, Birch, KMeans
import smaframework.tool.distribution as Distribution
import shapely, shapely.geometry, shapely.ops
from hdbscan import HDBSCAN

def _persistent_matches_formater(row, layer1, layer2):
    if row['beta_%s_timestamp' % layer1] > row['alpha_%s_timestamp' % layer1]:
        return [{
            'lat': row['alpha_%s_lat' % layer1],
            'lng': row['alpha_%s_lon' % layer1],
            'timestamp': row['alpha_%s_timestamp' % layer1],
            '%s_uid' % layer1: row['alpha_%s_uid' % layer1],
            '%s_uid' % layer2: row['alpha_%s_uid' % layer2],
            'distance': float("{:5.1f}".format(row['distance'])),
            'score_spatial': float("{:1.4f}".format(row['alpha_score_spatial'])),
            'score_temporal': float("{:1.4f}".format(row['alpha_score_temporal']))
        }, {
            'lat': row['beta_%s_lat' % layer1],
            'lng': row['beta_%s_lon' % layer1],
            'timestamp': row['beta_%s_timestamp' % layer1],
            '%s_uid' % layer1: row['beta_%s_uid' % layer1],
            '%s_uid' % layer2: row['beta_%s_uid' % layer2],
        }]
    else:
        return [{
            'lat': row['beta_%s_lat' % layer1],
            'lng': row['beta_%s_lon' % layer1],
            'timestamp': row['beta_%s_timestamp' % layer1],
            '%s_uid' % layer1: row['beta_%s_uid' % layer1],
            '%s_uid' % layer2: row['beta_%s_uid' % layer2],
            'distance': float("{:5.1f}".format(row['distance'])),
            'score_spatial': float("{:1.4f}".format(row['beta_score_spatial'])),
            'score_temporal': float("{:1.4f}".format(row['beta_score_temporal']))
        }, {
            'lat': row['alpha_%s_lat' % layer1],
            'lng': row['alpha_%s_lon' % layer1],
            'timestamp': row['alpha_%s_timestamp' % layer1],
            '%s_uid' % layer1: row['alpha_%s_uid' % layer1],
            '%s_uid' % layer2: row['alpha_%s_uid' % layer2],
        }]

def _persistent_matches_load_csv(args):
        path, file, layer1, layer2 = args
        df = pd.read_csv(os.path.join(path, file), header=0, low_memory=True, dtype={'twitter_uid': str, 'yellow_taxis_uid': str})
        return df[['%s_uid' % layer1, '%s_uid' % layer2, '%s_lat' % layer1, '%s_lon' % layer1, '%s_timestamp' % layer1, 'score_spatial', 'score_temporal']]

def persistent_matches(key, path, layer1, layer2, **kwargs):
    multiprocess = 'pool_size' in kwargs.keys() and int(kwargs['pool_size']) > 1
    if multiprocess:
        pool_size = int(kwargs['pool_size'])
        pool = mp.Pool(pool_size)
        results = pool.map(_persistent_matches_load_csv, [(path, file, layer1, layer2) for file in os.listdir(path) if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file)])
        pool.close()
        pool.join()
    else:
        results = []
        for file in os.listdir(path):
            if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file):
                results.append(_persistent_matches_load_csv((path, file, layer1, layer2)))

    df = pd.concat(results)
    if 'verbose' in kwargs.keys() and kwargs['verbose']:
        print("Loaded...", flush=True)

    if pd.__version__ >= '0.17.0':
        df.sort_values(by=['%s_uid' % layer1, '%s_uid' % layer2], inplace=True)
    else:
        df.sort(['%s_uid' % layer1, '%s_uid' % layer2], inplace=True)

    if 'verbose' in kwargs.keys() and kwargs['verbose']:
        print("Concatenated...", flush=True)
    
    df2 = df.shift(-1)
    df = pd.concat([df, df2], axis=1)
    df.columns = [
        'alpha_%s_uid' % layer1, 
        'alpha_%s_uid' % layer2, 
        'alpha_%s_lat' % layer1, 
        'alpha_%s_lon' % layer1,
        'alpha_%s_timestamp' % layer1,
        'alpha_score_spatial',
        'alpha_score_temporal',
        'beta_%s_uid' % layer1, 
        'beta_%s_uid' % layer2, 
        'beta_%s_lat' % layer1, 
        'beta_%s_lon' % layer1,
        'beta_%s_timestamp' % layer1,
        'beta_score_spatial',
        'beta_score_temporal',
        ]

    df = df[(df['alpha_%s_uid' % layer1] == df['beta_%s_uid' % layer1]) & (df['alpha_%s_uid' % layer2] == df['beta_%s_uid' % layer2])]
    df = df.drop_duplicates([
        'alpha_%s_lat' % layer1, 
        'alpha_%s_lon' % layer1,
        'beta_%s_lat' % layer1, 
        'beta_%s_lon' % layer1
        ])

    if 'verbose' in kwargs.keys() and kwargs['verbose']:
        print("Filtered...", flush=True)

    df['distance'] = df.apply(lambda r: distance.distance(Point("%f %f" % (r['alpha_%s_lat' % layer1], r['alpha_%s_lon' % layer1])), Point("%f %f" % (r['beta_%s_lat' % layer1], r['beta_%s_lon' % layer1]))).meters, axis=1)
    df['time_elapsed'] = df.apply(lambda r: abs(r['alpha_%s_timestamp' % layer1] - r['beta_%s_timestamp' % layer1]), axis=1)

    if 'min_distance' in kwargs.keys():
        min_distance = kwargs['min_distance']
    else:
        min_distance = 500

    if 'max_speed' in kwargs.keys():
        max_speed = 1 / kwargs['max_speed']
    else:
        max_speed = 0.036 # (1 / 100 Km/h) ~= (1 / 27.8 m/s)

    df = df[df['distance'] > min_distance]
    df = df[df['time_elapsed'] > max_speed * df['distance']] 
    
    if pd.__version__ >= '0.17.0':
        df.sort_values(by=['distance'], inplace=True)
    else:
        df.sort(['distance'], inplace=True)

    if 'verbose' in kwargs.keys() and kwargs['verbose']:
        print("Distance Filter...", flush=True)

    df = df.apply(lambda r: _persistent_matches_formater(r, layer1, layer2), axis=1)

    if 'verbose' in kwargs.keys() and kwargs['verbose']:
        print("Mapped...", flush=True)

    length = len(df)
    if length == 0:
        if 'verbose' in kwargs.keys() and kwargs['verbose']:
            print('Empty!')
        return

    result = json.dumps(df.tolist())

    with open('templates/google-polyline.html', 'r') as file:
        template = file.read()
    template = template.replace('<?=LIST?>', result).replace('<?=KEY?>', key)

    if 'filename' in kwargs.keys():
        filename = kwargs['filename'] % length
    else:
        filename = 'persistnet-matches-%d.html' % length

    with open('data/results/' + filename, 'w+') as outfile:
        outfile.write(template)

    if 'verbose' in kwargs.keys() and kwargs['verbose']:
        print("Done!", flush=True)

def heatpoint(args):
    filename, layer = args
    df = pd.read_csv(filename, header=0)

    spatial = df['score_spatial'].sum()
    temporal = df['score_temporal'].sum()
    lat = df['%s_lat' % layer].mean()
    lon = df['%s_lon' % layer].mean()

    return '{location: new google.maps.LatLng(%f, %f), weight: %f},' % (lat, lon, spatial + temporal)

def heatmap(key, path, layer, **kwargs):
    multiprocess = 'pool_size' in kwargs.keys() and int(kwargs['pool_size']) > 1
    if multiprocess:
        pool_size = int(kwargs['pool_size'])
        pool = mp.Pool(pool_size)
        results = pool.map(heatpoint, [(os.path.join(path, file), layer) for file in os.listdir(path) if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file)])
        pool.close()
        pool.join()
    else:
        results = []
        for file in os.listdir(path):
            if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file):
                results.append(heatpoint((os.path.join(path, file), layer)))
    results = '\n'.join(results)

    with open('templates/google-heatmap.html', 'r') as file:
        template = file.read()

    template = template.replace('<?=LIST?>', results).replace('<?=KEY?>', key)

    if 'filename' in kwargs.keys():
        filename = kwargs['filename']
    else:
        filename = 'heatmap-fuzzymatcher-' + IdGenerator.uuid4().hex + '.html'

    with open('data/results/' + filename, 'w+') as file:
        file.write(template)

def collect_matches(col, spatial_scores, layer1, layer2):
    count = 0
    values = col.tolist()
    for index in col.index.tolist():
        if values[count] > 0:
            spatial_scores.append((col.name, index, layer1['uid'][index], layer1['lat'][index], layer1['lon'][index], layer1['timestamp'][index], layer2['uid'][col.name], layer2['lat'][col.name], layer2['lon'][col.name], layer2['timestamp'][col.name], values[count]))
        count = count + 1

def spatial_item(node, precision, df):
    func = linear(precision)
    d = df.apply(lambda row: func(dist(row['lat'], row['lon'], node['lat'], node['lon'])), axis=1)
    d.columns = [node.index]
    return d

def temporal_item(node, precision, df):
    func = linear(precision)
    d = df.apply(lambda row: func(abs(row['timestamp'] - node['timestamp'])), axis=1)
    d.columns = [node.index]
    return d

def linear(x0):
    f = np.array([x0 for i in range(0, x0)])
    g = (np.arange(0, x0 + 1) * -1) + x0
    f = np.append(f, g)

    def func(x):
        x = int(x)
        return (f[x] if x < len(f) else 0) / x0

    return func

def dist(alat, alon, blat, blon):
    p1 = Point("%f %f" % (alat, alon))
    p2 = Point("%f %f" % (blat, blon))
    return distance.distance(p1, p2).meters

def analyse_cube(args):
    path, file, l1, l2, distance_precision, temporal_precision, config = args
    filename = os.path.join(path, file)

    try:
        layer, latb, lonb, timestampb = file.replace('.csv', '').split('-')
    except Exception as e:
        return None
    
    latb = int(latb)
    lonb = int(lonb)
    timestampb = int(timestampb)
    
    # load cube for layer1
    layer1 = pd.read_csv(filename, header=0, low_memory=False, memory_map=True, index_col='id')

    # load cubes for layer2
    dfs = []
    for i in [latb - 1, latb, latb + 1]:
        for j in [lonb - 1, lonb, lonb + 1]:
            for k in [timestampb - 1, timestampb, timestampb + 1]:
                f = os.path.join(path, '%s-%d-%d-%d.csv' % (l2, i, j, k))
                if not os.path.isfile(f):
                    continue

                dfs.append(pd.read_csv(f, header=0, low_memory=False, memory_map=True, index_col='id'))

    if len(dfs) == 0:
        # print('File %s analysed with no matching bucket...' % file)
        return None

    layer2 = pd.concat(dfs)

    # map distances
    spatial_scores = layer1.apply(lambda node: spatial_item(node, distance_precision, layer2), axis=1)
    spatial_scores = spatial_scores.loc[(spatial_scores.sum(axis=1) != 0), (spatial_scores.sum(axis=0) != 0)]

    # get distance matches
    ss = []
    spatial_scores.apply(lambda col: collect_matches(col, ss, layer1, layer2))

    if len(ss) == 0:
        # print('File %s analysed with no matching distances...' % file)
        return None

    spatial_scores = pd.DataFrame(ss, columns=['source', 'target', l1 + '_uid', l1 + '_lat', l1 + '_lon', l1 + '_timestamp', l2 + '_uid', l2 + '_lat', l2 + '_lon', l2 + '_timestamp', 'score'])
    
    # map times
    temporal_scores = layer1.apply(lambda node: temporal_item(node, temporal_precision, layer2), axis=1)
    temporal_scores = temporal_scores.loc[(temporal_scores.sum(axis=1) != 0), (temporal_scores.sum(axis=0) != 0)]

    # get time matches
    ts = []
    temporal_scores.apply(lambda col: collect_matches(col, ts, layer1, layer2))

    if len(ts) == 0:
        # print('File %s analysed with no matching times...' % file)
        return None

    temporal_scores = pd.DataFrame(ts, columns=['source', 'target', l1 + '_uid', l1 + '_lat', l1 + '_lon', l1 + '_timestamp', l2 + '_uid', l2 + '_lat', l2 + '_lon', l2 + '_timestamp', 'score'])

    # merge results
    df = pd.merge(spatial_scores, temporal_scores, on=['source', 'target'], suffixes=['_spatial', '_temporal'])
    df = df[['source', 'target', l1 + '_uid_spatial', l1 + '_lat_spatial', l1 + '_lon_spatial', l1 + '_timestamp_spatial', l2 + '_uid_spatial', l2 + '_lat_spatial', l2 + '_lon_spatial', l2 + '_timestamp_spatial',  'score_spatial', 'score_temporal']]
    df.columns = ['source', 'target', l1 + '_uid', l1 + '_lat', l1 + '_lon', l1 + '_timestamp', l2 + '_uid', l2 + '_lat', l2 + '_lon', l2 + '_timestamp', 'score_spatial', 'score_temporal']

    result_location = 'data/fuzzy-matches/'
    if not os.path.exists(result_location):
        try:
            os.makedirs(result_location)
        except Exception as e:
            pass

    if len(df.index):
        df.to_csv('data/fuzzy-matches/%s-%s-%s.csv' % (l1, l2, IdGenerator.uuid4().hex), index=False)

def analyze(path, distance_precision, temporal_precision, l1, l2, **kwargs):
    multiprocess = 'pool_size' in kwargs.keys() and int(kwargs['pool_size']) > 1
    if multiprocess:
        pool_size = int(kwargs['pool_size'])
        pool = mp.Pool(pool_size)
        pool.map(analyse_cube, [(path, file, l1, l2, distance_precision, temporal_precision, kwargs) for file in os.listdir(path) if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file)])
        pool.close()
        pool.join()
    else:
        for file in os.listdir(path):
            if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file):
                analyse_cube((path, file, l1, l2, distance_precision, temporal_precision, kwargs))

def load_matches_csv(args):
    path, file, counts = args
    df = pd.read_csv(os.path.join(path, file), header=0, low_memory=False, memory_map=True)
    counts.append(df.shape[0])
    return df

def clusterer(path, layer, epss, epst, **kwargs):
    # evaluate min_samples for clustering
    min_samples = 20 if 'min_samples' not in kwargs.keys() else kwargs['min_samples']

    # metric = 'seuclidean' if 'metric' not in kwargs.keys() else kwargs['metric']
    # metric_params = None if 'metric_params' not in kwargs.keys() else kwargs['metric_params']
    # algorithm for NN query {‘auto’, ‘ball_tree’, ‘kd_tree’, ‘brute’}
    # nnalgorithm = 'ball_tree' if 'nnalgorithm' not in kwargs.keys() else kwargs['nnalgorithm']

    # creating file for clusters
    cluster_dir = 'data/fuzzy-matches/clusters/'
    if not os.path.exists(cluster_dir):
        os.makedirs(cluster_dir)

    # load data
    if 'pool_size' in kwargs.keys() and int(kwargs['pool_size']) > 1:
        pool = mp.Pool(int(kwargs['pool_size']))
        counts = mp.Manager().list()
        result = pool.map(load_matches_csv, [(path, file, counts) for file in os.listdir(path) if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file)])
        pool.close()
        pool.join()
    else:
        kwargs['pool_size'] = 1
        result = []
        counts = []
        for file in os.listdir(path):
            if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file):
                result.append(load_matches_csv((path, file, counts)))

    # organize data
    frame = pd.concat(list(result))
    frame.reset_index(inplace=True)
    frame = frame[[layer + '_lat', layer + '_lon',  layer + '_timestamp', 'score_spatial', 'score_temporal']]
    frame.columns = ['lat', 'lon', 'timestamp', 'score_spatial', 'score_temporal']

    # hash for recovery
    cluster_hash = '-l%s-ms%d-epss%d-epst%d' % (layer, min_samples, epss, epst)
    if os.path.exists('%stotalizer%s.json' % (cluster_dir, cluster_hash)):
        return cluster_hash

    # epss meters to degrees
    earth_circumference = 2 * math.pi * distance.EARTH_RADIUS * 1000 # meters
    epss = epss * 360 / earth_circumference

    # using time scaling for ST-DBSCAN
    min_time = frame['timestamp'].min()
    frame['timestamp'] = (frame['timestamp'] - min_time) * epss / epst
    eps = epss

    # using space scaling for ST-DBSCAN
    # min_lat = frame['lat'].min()
    # min_lon = frame['lon'].min()
    # frame['lat'] = (frame['lat'] - min_lat) * epst / epss
    # frame['lon'] = (frame['lon'] - min_lon) * epst / epss
    # eps = epst

    ### cluster data and separate in frame
    clusterer = None
    fname = 'data/results/hdbscan%s.csv' % cluster_hash
    if os.path.isfile(fname):
        print('INFO: loading clusters')
        frame = pd.read_csv(fname)
    else:
        print('INFO: running ST-HDBSCAN')
        clusterer = HDBSCAN(min_samples=min_samples).fit(frame[['lat', 'lon', 'timestamp']].as_matrix())
        frame = pd.concat([frame, pd.DataFrame({'label': clusterer.labels_})], axis=1)
        frame = frame[frame['label'] != -1]
        frame.to_csv(fname)

    fname = 'data/results/kmeans%s.csv' % cluster_hash
    if os.path.isfile(fname):
        print('INFO: loading plot data')
        
        frame = pd.read_csv(fname)
    else:
        print('INFO: running KMEANS for ploting')
        n_clusters = int((frame['label'].max() - 1) * 0.1)
        clusterer = KMeans(n_clusters=n_clusters, n_jobs=int(kwargs['pool_size'])).fit(frame[['lat', 'lon']].as_matrix())

        frame = pd.concat([frame, pd.DataFrame({'label': clusterer.labels_})], axis=1)
        frame.to_csv(fname)

    frame = frame.groupby(by='label')

    for label, df in frame:
        if label == -1:
            continue
        df.to_csv('%s%d%s.csv' % (cluster_dir, label, cluster_hash))

    # get metadata about clusters
    totalizer = frame[['score_spatial', 'score_temporal']].mean()
    totalizer['count'] = frame['lat'].agg('count')
    totalizer = '{score_spatial: '+totalizer['score_spatial'].map(str)+', score_temporal: '+totalizer['score_temporal'].map(str)+', count: '+totalizer['count'].map(str)+'}'
    totalizer = totalizer.str.cat(sep=',')

    with open('%stotalizer%s.json' % (cluster_dir, cluster_hash), 'w+') as file:
        file.write(totalizer)

    return cluster_hash

def get_zones(key, path, layer, epss, epst, **kwargs):
    results_dir = 'data/results/'
    
    cluster_hash = clusterer(path, layer, epss, epst, **kwargs)

    # draw regions
    result = []
    cluster_dir = 'data/fuzzy-matches/clusters/'
    regex = re.compile('^(\d+)%s.csv$' % cluster_hash)
    if 'pool_size' in kwargs.keys() and int(kwargs['pool_size']) > 1:
        pool = mp.Pool(int(kwargs['pool_size']))
        result = pool.map(Distribution.get_region, [pd.read_csv(cluster_dir + filename) for filename in os.listdir(cluster_dir) if regex.match(filename)])
        pool.close()
        pool.join()
    else:
        for filename in os.listdir(cluster_dir):
            if regex.match(filename): 
                result.append(Distribution.get_region(pd.read_csv(cluster_dir + filename)))

    # create json for ploting on Google Maps
    print('INFO: creating plot object')
    regions = ''
    for region in result:
        df = '{lat: '+ region['lat'].map(str) +', lng: '+ region['lon'].map(str) +'}'
        json = '[' + df.str.cat(sep=',') + ']'
        regions = regions + json + ','

    # create HTML file with plot and finish
    with open('templates/google-shape.html', 'r') as file:
        template = file.read()

    with open('%stotalizer%s.json' % (cluster_dir, cluster_hash)) as file:
        totalizer = file.read()

    template = template.replace('<?=LIST?>', regions).replace('<?=KEY?>', key).replace('<?=DATA?>', totalizer)

    if 'filename' in kwargs.keys():
        filename = kwargs['filename']
    else:
        filename = 'regions-fuzzymatcher-' + IdGenerator.uuid4().hex + '.html'

    with open(results_dir + filename, 'w+') as file:
        file.write(template)

    print(results_dir + filename)
