from smaframework.common.env import env
import urllib.request
import requests
import json
import math

APP_KEY = env('HERE_APP_ID')
APP_CODE = env('HERE_APP_CODE')

'''
 * Run the request to collect traffic data.
 * 
 * @param query - the params to be sent in the querystring
'''
def extract_url(query):
    url = 'https://traffic.cit.api.here.com/traffic/6.2/flow.json?' + urllib.parse.urlencode(query)
    response = urllib.request.urlopen(url).read().decode("utf-8")
    return parse_response(json.loads(response))

'''
 * Gets traffic data in a corridor.
 * 
 * @param app_id - the APP_ID to be used in the request to HERE API
 * @param app_code - the APP_CODE to be used in the request to HERE API
 * @param path - a list of coordinates to form the corridor path
 * @param width - the width of the corridor
 * @param **kwargs - optional query params to be sent in the request
'''
def corridor(app_id, app_code, path, width, **kwargs):
    kwargs["app_id"]= app_id
    kwargs["app_code"]= app_code
    kwargs["corridor"] = ';'.join(['%f,%f' % tuple(position) for position in path]) + (';%d' % width)

    return extract_url(kwargs)

'''
 * Gets traffic data using the Mercator Projection.
 * 
 * @param app_id - the APP_ID to be used in the request to HERE API.
 * @param app_code - the APP_CODE to be used in the request to HERE API.
 * @param lat - the latitude of the desired traffic data.
 * @param lon - the longitude of the desired traffic data.
 * @param zoom - the zoom level of the desired traffic data from 0 to 21, where 0 is the whole earth and 21 is a specific location at building level (default=21).
'''
def lat_lon_zoom(app_id, app_code, lat, lon, zoom=21):
    latRad = lat * math.pi / 180;
    n = math.pow(2, zoom);
    x = n * ((lon + 180) / 360);
    y = n * (1-(math.log(math.tan(latRad) + 1/math.cos(latRad)) /math.pi)) / 2;

    params = {'app_code': app_code, 'app_id': app_id}

    request_url = 'https://traffic.cit.api.here.com/traffic/6.1/flow/json/%d/%d/%d' % (zoom, x, y)
    response = requests.get(request_url, params=params)

    obj = None
    try:
        return {
            'data': parse_response(json.loads(response.content.decode('utf8'))),
            'status_code': response.status_code
        }
    except Exception as e:
        return {
            'data': {},
            'status_code': response.status_code
        }


def point(lat, lon, r=50):
    if not _validate_key():
        return None

    result = {}
    prox = str(lat) + ',' + str(lon) + ',' + str(r)
    params = {'app_code': APP_CODE, 'app_id': APP_KEY, 'prox': prox}

    request_url = 'https://traffic.cit.api.here.com/traffic/6.1/flow.json'
    response = requests.get(request_url, params=params)
    result['status_code'] = response.status_code
    resp_json = json.loads(response.content.decode('utf8'))

    if 'error' in resp_json or 'Details' in resp_json:
        result['data'] = resp_json
        return result

    result['data'] = parse_response(resp_json)
    return result

def get_multiple_info_list(points, r=50):
    result = []
    for coords in points:
        result.append(point(coords[0], coords[1], r))

    return result

def _validate_key():
    return APP_KEY and APP_CODE

'''
 * Parse the response and keep only relevant data.
 * 
 * @param resp_json - the object received from the call of the HERE API
'''
def parse_response(resp_json):
    temp = {}

    for i1 in range(0, len(resp_json['RWS'])):
        resp_json_rws = resp_json['RWS'][i1]

        for i2 in range(0, len(resp_json_rws['RW'])):
            resp_json_rw = resp_json_rws['RW'][i2]

            for i3 in range(0, len(resp_json_rw['FIS'])):
                resp_json_fis = resp_json_rw['FIS'][i3]

                for i4 in range(0, len(resp_json_fis)):
                    resp_json_fi = resp_json_fis['FI'][i4]
                    resp_json_tmc = resp_json_fi['TMC']
                    resp_json_cf = resp_json_fi['CF'][0]

                    aux = {}
                    aux['DE'] = resp_json_tmc['DE']
                    aux['QD'] = resp_json_tmc['QD']
                    aux['JF'] = resp_json_cf['JF']
                    aux['CN'] = resp_json_cf['CN']

                    temp[str(resp_json_tmc['PC'])] = aux

    return temp