import smaframework.analyzer.bucketwalk.filesystem as BucketWalkFS
import smaframework.analyzer.magtools.mag as Mag
from haversine import haversine
from functools import partial
import multiprocessing as mp
import pandas as pd

"""
 * @param trips - list of trips retrieved from Google with smaframework.extractor.google.transit
 * @param index_path - path to the index used to store the data of busses and stops, created via smaframework.analyzer.bucketwalk.filesystem
 * @return trips - list of trips given containing for each trip a list of route options and for wach option a list of positions of the stops and changes
"""
def match(trips, index_path, **kwargs):
	if 'pool_size' not in kwargs.keys() or kwargs['pool_size'] == 1:
		return list(map(partial(match_trip, index_path), trips))
	
	with mp.Pool(processes=kwargs['pool_size']) as pool:
		return pool.map(partial(match_trip, index_path), trips)

def match_trip(index_path, trip):
	trip_points = []
	for route in trip:
		route_points = []
		for step in route:
			if step['travel_mode'] == 'WALKING':
				route_points.append(step['origin'])
				route_points.append(step['destination'])
			elif step['travel_mode'] == 'TRANSIT':
				layers = ['nyc_subway'] if step['vehicle_type'] == 'SUBWAY' else ['bus', 'express_buss', 'lirr', 'path']
				points = match_route(step['origin'], step['destination'], index_path, layers)
				route_points.extend(points)
		trip_points.append(route_points)
	return trip_points

def match_route(origin, destination, index_path, layers):
	dist = lambda p1, p2: haversine((p2['lat'], p2['lon']), p1)
	origin = BucketWalkFS.closest(index_path, {"lat": origin[0], "lon": origin[1]}, partial(dist, origin), layers=layers)
	destination = BucketWalkFS.closest(index_path, {"lat": destination[0], "lon": destination[1]}, partial(dist, destination), layers=layers)

	frame = pd.merge(origin, destination, how='inner', on=['uid'])
	origin = frame[['id_x', 'uid', 'timestamp_x', 'lat_x', 'lon_x', 'layer_x']]
	origin.columns = ['id', 'uid', 'timestamp', 'lat', 'lon', 'layer']
	destination = frame[['id_y', 'uid', 'timestamp_y', 'lat_y', 'lon_y', 'layer_y']]
	destination.columns = ['id', 'uid', 'timestamp', 'lat', 'lon', 'layer']

	return trace_path(origin, destination)

def trace_path(origin, destination):
	paths = []
	for i, row in origin.iterrows():
		o = row['id']
		d = destination['id'].loc[i]

		nodes = Mag.nodes_by('uid', row['uid'])
		ids = nodes['id']

		nodes.to_csv('data/nodes.csv')

		nodes = Mag.get_simple_path(ids, o, d)
		if not nodes.empty:
			paths.append(nodes)

	path = min(paths, key=lambda p: len(p))

	return path[['lat', 'lon']].as_matrix().tolist()