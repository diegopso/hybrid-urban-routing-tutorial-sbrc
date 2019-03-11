import os
import numpy as np
import pandas as pd
import datetime as dt
import uuid as IdGenerator

import multiprocessing as mp
import urllib
import json
import time

def extract_url(params):
    (app_key, departure, arrival, date, mode, kwargs) = params

    kwargs["origin"] = '%f,%f' % departure
    kwargs["destination"] = '%f,%f' % arrival
    kwargs["key"] = app_key
    kwargs["mode"] = mode
    kwargs["departure_time"] = '%d' % time.mktime(date.timetuple())

    url = 'https://maps.googleapis.com/maps/api/directions/json?' + urllib.parse.urlencode(kwargs)
    response = urllib.request.urlopen(url).read()
    response = json.loads(response.decode("utf-8"))

    routes = []
    for route in response['routes']:
        r = []
        for leg in route['legs']:
            for step in leg['steps']:
                r.append({
                    "travel_mode": step['travel_mode'],
                    "duration": step['duration']['value'], # seconds
                    "origin": (step['start_location']['lat'], step['start_location']['lng']),
                    "destination": (step['end_location']['lat'], step['end_location']['lng']),
                    "distance": step['distance']['value'], # meters
                    "vehicle_type": None if step['travel_mode'] != 'TRANSIT' else step['transit_details']['line']['vehicle']['type']
                    })
        routes.append(r)

    return routes

def extract(app_key, departures, arrivals, dates, mode, **kwargs):
    multiprocess = 'pool_size' in kwargs.keys() and int(kwargs['pool_size']) > 1
    if multiprocess:
        pool_size = int(kwargs['pool_size'])
        del kwargs['pool_size']
        pool = mp.Pool(pool_size)
        trips = pool.map(extract_url, [(app_key, departures[i], arrivals[i], dates[i], mode, kwargs) for i in range(0, len(departures))])
        pool.close()
        pool.join()
    else:
        trips = []
        for i in range(0, len(departures)):
            trips.append(extract_url((app_key, departures[i], arrivals[i], dates[i], mode, kwargs)))

    return trips

            

# def get_lines(trips, **kwargs):
    