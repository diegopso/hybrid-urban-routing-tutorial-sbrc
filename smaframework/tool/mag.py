import os
import math
import datetime
import pandas as pd
import uuid as IdGenerator
import multiprocessing as mp

def edges(path, edge_type=None, load_nodes=False, **kwargs):
    edges_path = os.path.join(path, 'edges/')
    if 'pool_size' in kwargs.keys() and int(kwargs['pool_size']) > 1:
        pool = mp.Pool(int(kwargs['pool_size']))
        result = pool.map(load_edges_csv, [(edges_path, file, edge_type) for file in os.listdir(edges_path) if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file)])
        pool.close()
        pool.join()
    else:
        result = []
        for file in os.listdir(edges_path):
            if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file):
                result.append(load_edges_csv((edges_path, file, edge_type)))

    df = pd.concat(result)

    if load_nodes:
        df = load_nodes_for_edges(df, os.path.join(path, 'nodes/'), **kwargs)

    return df

def nodes(path, layer=None, **kwargs):
    nodes_path = os.path.join(path, 'nodes/')
    if 'pool_size' in kwargs.keys() and int(kwargs['pool_size']) > 1:
        pool = mp.Pool(int(kwargs['pool_size']))
        result = pool.map(load_nodes_csv, [(nodes_path, file, layer) for file in os.listdir(nodes_path) if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file)])
        pool.close()
        pool.join()
    else:
        result = []
        for file in os.listdir(nodes_path):
            if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file):
                result.append(load_nodes_csv((nodes_path, file, layer)))

    return pd.concat(result)

def load_nodes_csv(args):
    path, file, layer = args
    df = pd.read_csv(os.path.join(path, file), header=0)
    
    if layer:
        return df[(df.layer == layer)]

    return df

def load_edges_csv(args):
    path, file, edge_type = args
    df = pd.read_csv(os.path.join(path, file), header=0)
    
    if edge_type:
        return df[(df.type == edge_type)]

    return df

def load_nodes_csv(args):
    path, file, layer = args
    df = pd.read_csv(os.path.join(path, file), header=0)
    
    if layer:
        return df[(df.layer == layer)]

    return df

def load_nodes_for_edges(edges, path, **kwargs):
    if 'pool_size' in kwargs.keys() and int(kwargs['pool_size']) > 1:
        pool = mp.Pool(int(kwargs['pool_size']))
        result = pool.map(load_nodes_csv, [(path, file, None) for file in os.listdir(path) if 'nodes_file_regex' not in kwargs.keys() or kwargs['nodes_file_regex'].match(file)])
        pool.close()
        pool.join()
    else:
        result = []
        for file in os.listdir(path):
            if 'nodes_file_regex' not in kwargs.keys() or kwargs['nodes_file_regex'].match(file):
                result.append(load_nodes_csv((path, file, None)))

    nodes = pd.concat(result)
    return (edges
            .merge(nodes, left_on='source', right_on='id', suffixes=('_edge', ''))
            .merge(nodes, left_on='target', right_on='id', suffixes=('_source', '_target'))
            .drop('id_source', axis=1)
            .drop('id_target', axis=1))

def add_node(node, **kwargs):
    if 'filename' not in kwargs.keys():
        filename = 'symulated-' + datetime.datetime.now().strftime('%Y-%m-%d') + '.csv'
    else:
        filename = kwargs['filename']

    filename = os.path.join('data/mag/nodes/', filename)
    if not os.path.isfile(filename):
        with open(filename, 'w+') as file:
            file.writelines('id,uid,timestamp,lat,lon,layer')

    node['id'] = IdGenerator.uuid4().hex

    with open(filename, 'a+') as file:
        file.write("\n%s,%s,%d,%f,%f,%s" % (node['id'], node['uid'],node['timestamp'],node['lat'],node['lon'],node['layer']))

    return node['id']

def add_edge(edge, **kwargs):
    if 'filename' not in kwargs.keys():
        filename = 'symulated-' + datetime.datetime.now().strftime('%Y-%m-%d') + '.csv'
    else:
        filename = kwargs['filename']

    filename = os.path.join('data/mag/edges/', filename)
    if not os.path.isfile(filename):
        with open(filename, 'w+') as file:
            file.writelines('source,target,id,type')

    edge['id'] = IdGenerator.uuid4().hex

    with open(filename, 'a+') as file:
        file.write("\n%s,%s,%s,%s" % (edge['source'], edge['target'],edge['id'],edge['type']))

    return edge['id']

def uid_entries_distribution(path, layer, **kwargs):    
    df = nodes(path, layer, **kwargs)
    df = df[['id', 'uid', 'timestamp']]

    day = 24 * 60 * 60
    df['timestamp'] = df.apply(lambda r: math.floor(r['timestamp'] / day), axis=1)

    df = df.groupby(['uid', 'timestamp']).count()
    per_uid_distribution = df['id'].reset_index()

    uid_amount = per_uid_distribution.copy().groupby(['timestamp']).count().reset_index()
    uid_amount = math.floor(uid_amount['uid'].mean())

    per_uid_distribution = per_uid_distribution.groupby(['id']).count().reset_index()[['id', 'uid']]
    sumation = per_uid_distribution['uid'].sum()
    per_uid_distribution['uid'] = per_uid_distribution['uid'].map(lambda r: r / sumation).to_frame()
    per_uid_distribution.columns = ['amount', 'probability']

    return {
        'amounts': per_uid_distribution['amount'].values,
        'probabilities': per_uid_distribution['probability'].values,
        'count': uid_amount
        }