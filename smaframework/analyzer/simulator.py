from math import floor
from geopy import Point, distance
import smaframework.tool.mag as Mag
import smaframework.tool.paralel as Paralel

def learn(path, layer, distance_precision=100, **kwargs):
    pool_size = 1 if 'pool_size' not in kwargs.keys() else kwargs['pool_size']
    
    nodes = Mag.nodes(path, layer, **kwargs)
    nodes = Paralel.prepare(nodes[['timestamp', 'lat', 'lon']], pool_size=pool_size)

    hour = 60 * 60
    day = 24 * hour
    week = 7 * day
    weekdays = 5 * day

    daytype_classifier = lambda timestamp: 'weekend' if timestamp % week > weekdays else 'weekday'
    hourly_classifier = lambda timestamp: floor((timestamp % day) / hour)

    nodes['day_type'] = nodes['timestamp'].map(daytype_classifier, meta=('day_type', str)).compute()
    nodes['hour'] = nodes['timestamp'].map(hourly_classifier, meta=('hour', int)).compute()
    
    min_lat = nodes['lat'].min().compute()
    min_lon = nodes['lon'].min().compute()
    origin = (min_lat, min_lon)
    
    nodes['lat'] = nodes['lat'].map(lambda x: floor(lat(origin, x) / distance_precision)).compute()
    nodes['lon'] = nodes['lon'].map(lambda x: floor(lon(origin, x) / distance_precision)).compute()

    print(nodes.head(10))

def lat(origin, l):
    p1 = Point("%f %f" % (origin[0], origin[1]))
    p2 = Point("%f %f" % (l, origin[1]))
    return distance.distance(p1, p2).meters

def lon(origin, l):
    p1 = Point("%f %f" % (origin[0], origin[1]))
    p2 = Point("%f %f" % (origin[0], l))
    return distance.distance(p1, p2).meters