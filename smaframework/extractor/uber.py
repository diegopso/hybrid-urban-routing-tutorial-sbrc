from smaframework.tool.constants import miles2km
import urllib.request
import json, math

'''
 * Estimate the duration and cost of a list of trips. Response provided using meters for distance, seconds for time and avarage cost for price.
 *
 * @param token - The Uber API token to be used in the request
 * @param departures - The position (lat, lon) of departure of the trip
 * @param arrivals - The position (lat, lon) of arrival of the trip
 * @param seat_counts - The amount of travelers
 * @param modality - The Uber modality (e.g. uberX, uberXL, POOL)
'''
def extract(token, departures, arrivals, seat_counts=None, modality='uberX', **kwargs):
    if seat_counts == None:
        seat_counts = [1] * len(departures)

    multiprocess = 'pool_size' in kwargs.keys() and int(kwargs['pool_size']) > 1
    if multiprocess:
        pool_size = int(kwargs['pool_size'])
        pool = mp.Pool(pool_size)
        trips = pool.map(_function_capsule, [(estimate, token, departures[i], arrivals[i], seat_counts[i], modality) for i in range(0, len(departures))])
        pool.close()
        pool.join()
    else:
        trips = []
        for i in range(0, len(departures)):
            trips.append(estimate(token, departures[i], arrivals[i], seat_counts[i], modality))

    return trips

def _function_capsule(params):
    fn, token, departure, arrival, seat_count, modality = params
    return fn(token, departure, arrival, seat_count, modality)

'''
 * Estimate the duration and cost of a trip. Response provided using meters for distance, seconds for time and avarage cost for price.
 *
 * @param token - The Uber API token to be used in the request
 * @param departure - The position (lat, lon) of departure of the trip
 * @param arrival - The position (lat, lon) of arrival of the trip
 * @param seat_count - The amount of travelers
 * @param modality - The Uber modality (e.g. uberX, uberXL, POOL)
'''
def estimate(token, departure, arrival, seat_count=1, modality='uberX'):
    estimates = get_url(token, 'https://api.uber.com/v1.2/estimates/price?', {
        "start_latitude": departure[0],
        "start_longitude": departure[1],
        "end_latitude": arrival[0],
        "end_longitude": arrival[1],
        "seat_count": seat_count,
        })

    trip = {}
    for estimate in estimates['prices']:
        if estimate['display_name'] == modality:
            trip = {
                "distance": math.ceil(estimate['distance'] * miles2km * 1000),
                "duration": estimate['duration'],
                "currency": estimate['currency_code'],
                "price": math.ceil((estimate['low_estimate'] + estimate['high_estimate']) / 2)
            }

            break

    waits = get_url(token, 'https://api.uber.com/v1.2/estimates/time?', {
        "start_latitude": departure[0],
        "start_longitude": departure[1]
        })

    for estimate in waits['times']:
        if estimate['display_name'] == modality:
            trip['wait'] = estimate['estimate']
            break

    trip['origin'] = departure
    trip['destination'] = arrival
    trip['phase'] = 'headway'
    return trip

def get_url(token, url, params):
    request = urllib.request.Request(url + urllib.parse.urlencode(params))
    request.add_header('Authorization', 'Token %s' % token)
    request.add_header('Accept-Language', 'en_US')
    request.add_header('Content-Type', 'application/json')
    response = urllib.request.urlopen(request).read().decode("utf-8")
    return json.loads(response)