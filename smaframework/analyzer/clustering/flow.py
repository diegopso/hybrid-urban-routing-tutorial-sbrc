import smaframework.tool.distribution as Distribution
from sklearn.neighbors import NearestNeighbors
from sklearn.cluster import DBSCAN
from hdbscan import HDBSCAN
import pandas as pd
import numpy as np
import sklearn, json

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

def cluster_hdbscan(filename, origin_columns, destination_columns, **kwargs):
    frame = pd.read_csv(filename, header=0, low_memory=True)
    output_file = kwargs['output_file'] if 'output_file' in kwargs.keys() else 'data/results/flow-cluster-' + IdGenerator.uuid4().hex
    pool_size = int(kwargs['pool_size']) if 'pool_size' in kwargs.keys() else 1
    gmaps_key = kwargs['gmaps_key'] if 'gmaps_key' in kwargs.keys() else False
    min_size = kwargs['min_size'] if 'min_size' in kwargs.keys() else int(len(frame)/1000)

    frame = clusterize_hdbscan(frame, origin_columns, destination_columns, min_size, pool_size)

    return summarize_data(frame, gmaps_key, output_file, origin_columns, destination_columns)

def cluster(filename, origin_columns, destination_columns, **kwargs):
    frame = pd.read_csv(filename, header=0, low_memory=True)

    min_samples = 15 if 'min_samples' not in kwargs.keys() else kwargs['min_samples']
    nnalgorithm = 'ball_tree' if 'nnalgorithm' not in kwargs.keys() else kwargs['nnalgorithm'] # algorithm for NN query {‘auto’, ‘ball_tree’, ‘kd_tree’, ‘brute’}
    output_file = kwargs['output_file'] if 'output_file' in kwargs.keys() else 'data/results/flow-cluster-' + IdGenerator.uuid4().hex
    pool_size = int(kwargs['pool_size']) if 'pool_size' in kwargs.keys() else 1
    gmaps_key = kwargs['gmaps_key'] if 'gmaps_key' in kwargs.keys() else False

    if 'eps' in kwargs.keys():
        eps_origin = kwargs['eps']
        eps_destination = kwargs['eps']
    else:
        sharpener = len(frame) / 1000
        eps_origin = select_eps(frame[origin_columns], min_samples) / sharpener
        eps_destination = select_eps(frame[destination_columns], min_samples) / sharpener
    
    print('INFO: eps(origin=%f, destination=%f) for file=%s' % (eps_origin, eps_destination, output_file))

    frame = clusterize(frame, eps_origin, eps_destination, min_samples, origin_columns, destination_columns, nnalgorithm, pool_size)

    return summarize_data(frame, gmaps_key, output_file, origin_columns, destination_columns, {
            'min_samples': float(min_samples),
            'eps_origin': float(eps_origin),
            'eps_destination': float(eps_destination)
            })

def summarize_data(frame, gmaps_key, output_file, origin_columns, destination_columns, metadata={}):
    frame.to_csv(output_file + '.csv')

    origin_frame = frame.groupby('labels_origin')
    destination_frame = frame.groupby('labels_destination')
    flow_frame = frame.groupby(['labels_origin', 'labels_destination'])
    
    result = []
    flows = []
    for (group, df) in flow_frame:
        if group[0] == -1 or group[1] == -1:
            continue

        origin = origin_frame.get_group(group[0])
        origin_region = get_region(origin, origin_columns)
        origin_centroid = origin.mean()

        destination = destination_frame.get_group(group[1])
        destination_region = get_region(destination, destination_columns)
        destination_centroid = destination.mean()

        item = {}
        for key in origin_columns:
            item[key] = origin_centroid[key]

        for key in destination_columns:
            item[key] = destination_centroid[key]

        item['flow'] = len(df)

        result.append(item)

        if gmaps_key:
            flow = {
                'weight': len(df),
                'origin_region_id': int(group[0]),
                'destination_region_id': int(group[1]),
                'origin_centroid': {
                    'lat': origin_centroid[origin_columns[0]],
                    'lng': origin_centroid[origin_columns[1]]
                },
                'destination_centroid': {
                    'lat': destination_centroid[destination_columns[0]],
                    'lng': destination_centroid[destination_columns[1]]
                },
                'origin_region': json.loads(origin_region),
                'destination_region': json.loads(destination_region),
                'link': [{
                    'lat': origin_centroid[origin_columns[0]],
                    'lng': origin_centroid[origin_columns[1]]
                }, {
                    'lat': destination_centroid[destination_columns[0]],
                    'lng': destination_centroid[destination_columns[1]]
                }]
            }

            flows.append(flow)

    frame = pd.DataFrame(result)

    if pd.__version__ >= '0.17.0':
        flow_thershold = select_knee(frame['flow'].sort_values().values)
    else:
        flow_thershold = select_knee(frame['flow'].sort().values)

    print('INFO: flow_thershold=%f for file=%s' % (flow_thershold, output_file))

    frame = frame[frame['flow'] > flow_thershold]

    if gmaps_key:
        flows = list(filter(lambda flow: flow['weight'] >= flow_thershold, flows))

        with open('templates/google-flow.html', 'r') as file:
            template = file.read()
        
        template = template.replace('<?=FLOWS?>', json.dumps(flows)).replace('<?=KEY?>', gmaps_key)
        
        with open(output_file + '.html', 'w+') as outfile:
            outfile.write(template)

        with open(output_file + '.json', 'w+') as outfile:
            json.dump(flows, outfile)

    metadata['flow_thershold'] = float(flow_thershold)
    with open(output_file + '.metadata.json', 'w+') as outfile:
        json.dump(metadata, outfile)

    return frame

def get_region(df, columns):
    df = df[columns]
    df.columns = ['lat', 'lon']
    df = Distribution.get_region(df)
    df = '{"lat": '+ df['lat'].map(str) +', "lng": '+ df['lon'].map(str) +', "teta": '+ df['teta'].map(str) +'}'
    return '[' + df.str.cat(sep=',') + ']'

# from: https://www.quora.com/What-is-the-mathematical-characterization-of-a-%E2%80%9Cknee%E2%80%9D-in-a-curve
def select_knee(y):
    try:
        dy = np.gradient(y)
        ddy = np.gradient(dy)
        
        x = np.arange(len(y))
        dx = np.gradient(x)
        ddx = np.gradient(dx)

        k = np.absolute(dx*ddy-dy*ddx) / np.power(dx*dx+dy*dy, 3/2)
        dk = np.gradient(k)

        return y[np.argmin(dk)]
    except Exception as e:
        print(len(y))
        return y[int(len(y) / 2)]

def clusterize_hdbscan(frame, origin_columns, destination_columns, min_size, pool_size=1):
    print('INFO: running HDBSCAN')
    clusterer_origin = HDBSCAN(min_cluster_size=min_size).fit(frame[origin_columns].values)
    clusterer_destination = HDBSCAN(min_cluster_size=min_size).fit(frame[destination_columns].values)
    print('INFO: finished HDBSCAN with nclusters(origin=%d, destination=%d)' % (int(clusterer_origin.labels_.max()), int(clusterer_destination.labels_.max())))
    return pd.concat([frame, pd.DataFrame({'labels_origin': clusterer_origin.labels_, 'labels_destination': clusterer_destination.labels_})], axis=1)

def clusterize(frame, eps_origin, eps_destination, min_samples, origin_columns, destination_columns, nnalgorithm='ball_tree', pool_size=1):
    clusterer_origin = None
    clusterer_destination = None
    
    print('INFO: running DBSCAN')
    if sklearn.__version__ > '0.15.2':
        print("\033[93mWARNING: in case of high memory usage error, downgrade scikit: `pip install scikit-learn==0.15.2`\033[0m")
        clusterer_origin = DBSCAN(eps=eps_origin, min_samples=min_samples, n_jobs=pool_size, algorithm=nnalgorithm).fit(frame[origin_columns].as_matrix())
        clusterer_destination = DBSCAN(eps=eps_destination, min_samples=min_samples, n_jobs=pool_size, algorithm=nnalgorithm).fit(frame[destination_columns].as_matrix())
    else:
        clusterer_origin = DBSCAN(eps=eps_origin, min_samples=min_samples).fit(frame[origin_columns].as_matrix())
        clusterer_destination = DBSCAN(eps=eps_destination, min_samples=min_samples).fit(frame[destination_columns].as_matrix())
    print('INFO: finished DBSCAN with nclusters(origin=%d, destination=%d)' % (int(clusterer_origin.labels_.max()), int(clusterer_destination.labels_.max())))

    return pd.concat([frame, pd.DataFrame({'labels_origin': clusterer_origin.labels_, 'labels_destination': clusterer_destination.labels_})], axis=1)

def select_eps(frame, min_samples):
    nbrs = NearestNeighbors(n_neighbors=min_samples).fit(frame)
    distances, indices = nbrs.kneighbors(frame)
    distances = distances[:,distances.shape[1] - 1]
    distances.sort()
    return select_knee(distances)
