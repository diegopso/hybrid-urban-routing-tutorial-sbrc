import smaframework.analyzer.bucketwalk.memory as BucketWalk
from pyproj import Proj, transform
from haversine import haversine
import fiona, os, re
import uuid as IdGenerator
import pandas as pd
import numpy as np
import multiprocessing as mp
import json

def get_route(params):
    feature, stops, inProj, outProj, kwargs = params
    error = 0.05
    route = []
    for segment in feature['geometry']['coordinates']:
        if any(isinstance(i, tuple) for i in segment):
            for p in segment:
                point = tuple(reversed(transform(inProj, outProj, p[0], p[1])))
                route.append(point)
        else:
            point = tuple(reversed(transform(inProj, outProj, segment[0], segment[1])))
            route.append(point)

    if 'index' in kwargs.keys():
        index = kwargs['index']
    else:
        index = BucketWalk.in_memory([stop['location'] for stop in stops])

    line_stops = []
    route = clean_route(route)

    for p in route:
        i, distance = BucketWalk.closest(index, p)

        if distance < error:
            line_stops.append(stops[i])

    seen = []
    remove = []
    for i, s in enumerate(line_stops):
        id_prop = 'stop_id' if 'stop_id' in s['properties'].keys() else 'station_id'
        if s['properties'][id_prop] in seen:
            remove.append(i)
        else:
            seen.append(s['properties'][id_prop])

    for r in sorted(remove, reverse=True):
        del line_stops[r]
    
    return {
        "path": route,
        "stops": line_stops,
        "properties": feature['properties']
    }

def clean_route(route):
    cleaned = []
    for i in range(0, len(route)):
        if i > 0 and route[i][0] == route[i-1][0] and route[i][1] == route[i-1][1]:
            continue
        cleaned.append(route[i])

    if i > 0 and route[0][0] == route[len(cleaned) - 1][0] and route[0][1] == route[len(cleaned) - 1][1]:
        del cleaned[len(cleaned) - 1]

    return cleaned

def transit_routes(routes_filepath, stops_filepath, layer, inproj='epsg:2263', outproj='epsg:4326', **kwargs):
    inProj = Proj(init=inproj, preserve_units = True)
    outProj = Proj(init=outproj)

    stops = [{
        "location": tuple(reversed(transform(inProj, outProj, f['geometry']['coordinates'][0], f['geometry']['coordinates'][1]))), 
        "properties": f['properties']
    } for f in fiona.open(stops_filepath)]

    index = BucketWalk.in_memory([stop['location'] for stop in stops])

    if 'pool_size' in kwargs.keys() and int(kwargs['pool_size']) > 1:
        pool = mp.Pool(int(kwargs['pool_size']))
        routes = pool.map(get_route, [(feature, stops, inProj, outProj, {"index": index}) for feature in fiona.open(routes_filepath)])
        pool.close()
        pool.join()
    else:
        routes = [get_route((feature, stops, inProj, outProj, {"index": index})) for feature in fiona.open(routes_filepath)]

    if 'uid_property' not in kwargs.keys():
        kwargs['uid_property'] = 'route_shor'

    if 'geoid_property' not in kwargs.keys():
        kwargs['geoid_property'] = 'GEOID'

    df = []
    for route in routes:
        i = 0
        for stop in route['stops']:
            df.append([IdGenerator.uuid4().hex, route['properties'][kwargs['uid_property']], i, stop['location'][0], stop['location'][1], layer, stop['properties'][kwargs['geoid_property']]])
            i = i + 1
    df = pd.DataFrame.from_records(df, columns=['id', 'uid', 'timestamp', 'lat', 'lon', 'layer', 'geoid'])

    if 'filename' not in kwargs.keys():
        kwargs['filename'] = 'data/entries/%s.csv' % layer

    if not os.path.exists('data/entries/'):
        os.makedirs('data/entries/')

    if 'batch_size' in kwargs.keys():
        for g, frame in df.groupby(np.arange(len(df)) // kwargs['batch_size']):
            frame.to_csv(re.sub(r'\.(.*)$', str(g) + r'.\1', kwargs['filename']), index=False)
    else:
        df.to_csv(kwargs['filename'], index=False)

    return routes

