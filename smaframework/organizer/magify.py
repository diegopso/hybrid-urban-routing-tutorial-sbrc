import os
import multiprocessing as mp
import pandas as pd
import uuid as IdGenerator

def organize_file(filename, edge_type, config):
    mag_location = 'data/mag/'
    nodes_location = 'data/mag/nodes/'
    edges_location = 'data/mag/edges/'

    if not os.path.exists(mag_location):
        os.makedirs(mag_location)
    if not os.path.exists(nodes_location):
        os.makedirs(nodes_location)
    if not os.path.exists(edges_location):
        os.makedirs(edges_location)

    # read checkpoints
    df = pd.read_csv(filename, header=0)

    if 'columns' not in config.keys():
        config['columns'] = ['id', 'uid', 'timestamp', 'lat', 'lon', 'layer']

    # add ids to nodes
    iddf = df.apply(lambda x: IdGenerator.uuid4().hex, axis = 1)
    df['id'] = iddf
    df = df[config['columns']]

    # save nodes to disk
    if len(df.index):
        df.to_csv(nodes_location + IdGenerator.uuid4().hex + '.csv', index=False)
    else:
        return True

    # organize and filter nodes to create edges
    if pd.__version__ >= '0.17.0':
        df.sort_values(by=['uid', 'timestamp'], ascending=[1,1], inplace=True)
    else:
        df.sort(['uid', 'timestamp'], ascending=[1,1], inplace=True)

    df.reset_index(drop=True, inplace=True)
    df = df[['id', 'uid']]

    # match nodes to create edges
    df2 = df.shift(-1)
    df2.columns = ['id2','uid2']
    df = pd.concat([df, df2], axis=1)
    df = df[df.uid == df.uid2]
    
    # fillter nodes data
    df = df[['id', 'id2']]
    df.columns = ['source', 'target']

    # add missing data to edges
    iddf = df.apply(lambda x: IdGenerator.uuid4().hex, axis = 1)
    df['id'] = iddf
    df['type'] = edge_type

    # save edges to disk
    if len(df.index):
        df.to_csv(edges_location + IdGenerator.uuid4().hex + '.csv', index=False)
    return True

def organize(path, edge_type, **kwargs):
    multiprocess = 'pool_size' in kwargs.keys() and int(kwargs['pool_size']) > 1
    if multiprocess:
        pool_size = int(kwargs['pool_size'])
        pool = mp.Pool(pool_size)

    filelist = os.listdir(path)
    for file in filelist:
        if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file):
            if multiprocess:
                pool.apply_async(organize_file, args=(os.path.join(path, file), edge_type, kwargs))
            else:
                organize_file(os.path.join(path, file), edge_type, kwargs)

    if multiprocess:
        pool.close()
        pool.join()