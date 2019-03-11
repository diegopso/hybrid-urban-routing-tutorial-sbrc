import numpy as np
import pandas as pd
import datetime as dt
import uuid as IdGenerator
import multiprocessing as mp
import os, urllib, json, time, re
from smaframework.common.address_keywords_extension_map import address_keywords_extensions
from smaframework.common.address_keywords_extension_map import parse_str as parse_address_str

'''
 * Obtain the suggested routes for a given departure, arival, date and mode. The params are given as a single tuple.
 *
 * @param app_key - The Google API key to perform the request.
 * @param departure - The location (lat, lon) of departure.
 * @param arrival - The location (lat, lon) of arrival.
 * @param date - The departure time.
 * @param mode - The travel mode (e.g., TRANSIT, WALKING, DRIVING).
 * @param kwargs - Other optional params as a dict.
'''
def extract_url(params):
    (app_key, departure, arrival, date, mode, prices, kwargs) = params

    kwargs["origin"] = '%f,%f' % tuple(departure)
    kwargs["destination"] = '%f,%f' % tuple(arrival)
    kwargs["key"] = app_key
    kwargs["mode"] = mode
    kwargs["alternatives"] = "true"
    kwargs["departure_time"] = str(date) if isinstance(date, int) else '%d' % time.mktime(date.timetuple())
    kwargs["units"] = 'metric' if "units" not in kwargs.keys() else kwargs["units"]

    url = 'https://maps.googleapis.com/maps/api/directions/json?' + urllib.parse.urlencode(kwargs)
    response = urllib.request.urlopen(url).read().decode("utf-8")
    response = json.loads(response)

    if response['status'] == 'OVER_QUERY_LIMIT':
        raise ValueError('Google says: You have exceeded your daily request quota for this API.')

    routes = []
    for route in response['routes']:
        r = []
        for leg in route['legs']:
            arrival_time = int(kwargs["departure_time"])
            for i, step in enumerate(leg['steps']):
                step = parse_step(step, prices, arrival_time)
                
                step['phase'] = 'headway' if mode != 'transit' or step['travel_mode'] == 'TRANSIT' else ('egress' if i == len(leg['steps']) - 1 else 'access')
                if step['phase'] == 'access':
                    step['next_mode'] = leg['steps'][i+1]['transit_details']['line']['vehicle']['type']

                arrival_time = arrival_time + step['duration'] + step['wait']
                r.append(step)
        routes.append(r)

    return routes

'''
 * Obtain the suggested routes for a given departure, arival, date and mode. The params are given as a single tuple.
 *
 * @param app_key - The Google API key to perform the request.
 * @param departure - The location (lat, lon) of departure.
 * @param arrival - The location (lat, lon) of arrival.
 * @param date - The departure time.
 * @param mode - The travel mode (e.g., TRANSIT, WALKING, DRIVING).
 * @param kwargs - Other optional params as a dict.
'''
def extract_single(app_key, departure, arrival, date, mode, prices, **kwargs):
    return extract_url((app_key, departure, arrival, date, mode, prices, kwargs))

'''
 * Parses a step in a Google Route to collect relevant data.
 *
 * @param step - the object representing the step.
'''
def parse_step(step, prices={}, arrival_time=0):
    address_keywords = []
    if step['travel_mode'] == 'DRIVING':
        matches = re.findall(r"<b>(.*?)</b>", step['html_instructions'])
        if len(matches) == 0:
            address_keywords = []
        else:
            address_keywords = []
            for m in matches:
                if len(m.split()) > 1:
                    address_keywords.extend(parse_address_str(m))

    vehicle_type = None if step['travel_mode'] != 'TRANSIT' else step['transit_details']['line']['vehicle']['type']
    wait = step['transit_details']['departure_time']['value'] - arrival_time if step['travel_mode'] == 'TRANSIT' else 0

    return {
        "travel_mode": step['travel_mode'],
        "duration": step['duration']['value'], # seconds
        "wait": wait, # seconds
        "origin": (step['start_location']['lat'], step['start_location']['lng']),
        "destination": (step['end_location']['lat'], step['end_location']['lng']),
        "distance": step['distance']['value'], # meters
        "vehicle_type": vehicle_type,
        "address_keywords": address_keywords,
        "price": prices[step['travel_mode']] if step['travel_mode'] in prices.keys() else 0
        }

'''
 * Get the Google suggested routes for a list of departures, arrivals and dates.
 *
 * @param app_key - The Google API key to perform the request.
 * @param departures - The list of locations (lat, lon) of departure.
 * @param arrivals - The list of locations (lat, lon) of arrival.
 * @param dates - The list of departure times.
 * @param mode - The travel mode (e.g., TRANSIT, WALKING, DRIVING).
 * @param kwargs - Other optional params as a dict.
'''
def extract(app_key, departures, arrivals, dates, mode, prices, **kwargs):
    multiprocess = 'pool_size' in kwargs.keys() and int(kwargs['pool_size']) > 1
    if multiprocess:
        pool_size = int(kwargs['pool_size'])
        del kwargs['pool_size']
        pool = mp.Pool(pool_size)
        trips = pool.map(extract_url, [(app_key, departures[i], arrivals[i], dates[i], mode, prices, kwargs) for i in range(0, len(departures))])
        pool.close()
        pool.join()
    else:
        trips = []
        for i in range(0, len(departures)):
            trips.append(extract_url((app_key, departures[i], arrivals[i], dates[i], mode, prices, kwargs)))

    return trips