import os, re
import pandas as pd

_mag = None

def load(path='data/mag/', **kwargs):
    global _mag 
    
    if 'target' in kwargs.keys() and kwargs['target']:
        _mag = kwargs['target']
    else:
        _mag = {}

    if 'file_regex' not in kwargs.keys():
        kwargs['file_regex'] = re.compile(r"^(.*)\.csv$")

    if 'nodes' not in _mag.keys():
        frames = []
        nodes_path = os.path.join(path, 'nodes')
        for file in os.listdir(nodes_path):
            if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file):
                frames.append(pd.read_csv(os.path.join(nodes_path, file)))
        _mag['nodes'] = pd.concat(frames, axis=0, ignore_index=True)

    if 'edges' not in _mag.keys():
        frames = []
        edges_path = os.path.join(path, 'edges')
        for file in os.listdir(edges_path):
            if 'file_regex' not in kwargs.keys() or kwargs['file_regex'].match(file):
                frames.append(pd.read_csv(os.path.join(edges_path, file)))
        _mag['edges'] = pd.concat(frames, axis=0, ignore_index=True)

    return _mag

def nodes_by(prop, value, **kwargs):
    path = 'data/mag/' if 'mag_dir' not in kwargs.keys() else kwargs['mag_dir']

    if not _mag:
        load(path, **kwargs)

    frame = _mag['nodes']
    return frame[frame[prop] == value]
    
def get_simple_path(ids, start=None, end=None, **kwargs):
    path = 'data/mag/' if 'mag_dir' not in kwargs.keys() else kwargs['mag_dir']

    if not _mag:
        load(path, **kwargs)

    frame = _mag['edges']

    edges = frame[frame['source'].isin(ids) | frame['target'].isin(ids)]

    if not start or not end:
        return edges

    edges.to_csv('data/edges.csv')

    target = edges[edges['source'] == start]
    path = []

    while (not target['source'].empty) and target['source'].values[0] != end:
        source = target
        path.append(source)
        target = edges[edges['source'] == source['target'].values[0]]

    if not target['source'].empty:
        frame = pd.concat(path, axis=0, ignore_index=True)
        frame = pd.concat([frame['source'], frame['target']], axis=1).stack().reset_index(drop=True)
        frame.drop_duplicates(inplace=True)
        return _mag['nodes'][_mag['nodes']['id'].isin(frame)]

    target = edges[edges['target'] == start]
    path = []

    while (not target['target'].empty) and target['target'].values[0] != end:
        source = target
        path.append(source)
        target = edges[edges['target'] == source['source'].values[0]]

    if target['target'].empty:
        return pd.DataFrame()

    frame = pd.concat(path, axis=0, ignore_index=True)
    frame = pd.concat([frame['source'], frame['target']], axis=1).stack().reset_index(drop=True)
    frame.drop_duplicates(inplace=True)
    return _mag['nodes'][_mag['nodes']['id'].isin(frame)]