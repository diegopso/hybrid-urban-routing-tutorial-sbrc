import urllib.request
import xmltodict, json, sys
from urllib.parse import quote
import smaframework.tool.conversor as Conversor

def parse(response):
    if 'calculateRouteResponse' not in response.keys() or 'route' not in response['calculateRouteResponse'].keys():
        return None

    routes = response['calculateRouteResponse']['route']

    if not isinstance(routes, list):
        routes = [routes]

    for route in routes:
        points = route['leg']['points']['point']

        for p in points:
            p['@latitude'] = float(p['@latitude'])
            p['@longitude'] = float(p['@longitude'])

        pointer = 0
        size = len(points)
        instruction_indexes = []
        for (index, instruction) in enumerate(route['guidance']['instructions']['instruction']):
            for i in range(pointer, size):
                instruction['routeOffsetInMeters'] = int(instruction['routeOffsetInMeters'])
                instruction['travelTimeInSeconds'] = int(instruction['travelTimeInSeconds'])
                instruction['point']['@latitude'] = float(instruction['point']['@latitude'])
                instruction['point']['@longitude'] = float(instruction['point']['@longitude'])

                if instruction['point']['@latitude'] == points[i]['@latitude'] and instruction['point']['@longitude'] == points[i]['@longitude']:
                    route['guidance']['instructions']['instruction'][index]['point']['@index'] = i
                    instruction_indexes.append(index)
                    pointer = i
                    break

        if 'sections' not in route.keys():
            route['sections'] = {
                'section': []
            }
            continue


        if not isinstance(route['sections']['section'], list):
            route['sections']['section'] = [route['sections']['section']]

        for (i, section) in enumerate(route['sections']['section']):
            if section['sectionType'] != 'TRAFFIC':
                continue

            section['startPointIndex'] = int(section['startPointIndex'])
            section['endPointIndex'] = int(section['endPointIndex'])
            section['effectiveSpeed'] = Conversor.kmph2mps(float(section['effectiveSpeedInKmh']))
            section['delayInSeconds'] = int(section['delayInSeconds'])
            section['magnitudeOfDelay'] = int(section['magnitudeOfDelay'])

            section['startPoint'] = points[section['startPointIndex']]
            section['endPoint'] = points[section['endPointIndex']]

            start = section['startPointIndex']
            end = section['endPointIndex']

            index = 0
            if len(instruction_indexes) > 0:
                index = min(instruction_indexes, key=lambda x: abs(x - start))
            route['sections']['section'][i]['startInstructionIndex'] = index

            if len(instruction_indexes) > 0:
                index = min(instruction_indexes, key=lambda x: abs(x - end))
            route['sections']['section'][i]['endInstructionIndex'] = index

    return routes

def getRoute(origin, destination, key, maxAlternatives=0, parseData=True, log='debug', **kwargs):
    '''
     * Obtain the suggested routes for a given origin, and destination.
     *
     * @param origin            The Google API key to perform the request.
     * @param destination       The location (lat, lon) of departure.
     * @param key               The TomTom API key.
     * @param maxAlternatives   The maximum amount of route alternatives to be retrieved.
     * @param parseData         Wether to parse the data according to a convenient format or return it raw.
     * @param kwargs            Other optional params as a dict.
     *                          - travelMode (default: car; options: car, truck, taxi, bus, van, motorcycle, bicycle, pedestrian)
    '''
    config = {
        'travelMode': 'car',
    }
    config.update(kwargs)

    query = '%f,%f' % origin + ':%f,%f' % destination
    query = quote(query)
    url = 'https://api.tomtom.com/routing/1/calculateRoute/%s?key=%s&sectionType=traffic&instructionsType=coded&maxAlternatives=%d&traffic=true&travelMode=%s' % (query, key, maxAlternatives, config['travelMode'])

    response = ''
    try:
        response = urllib.request.urlopen(url).read().decode("utf-8")
        response = xmltodict.parse(response)

        if log == 'debug':
            print('DEBUG TOMTOM: %s' % json.dumps(response))
    except urllib.error.HTTPError as e:
        error = xmltodict.parse(e.read())
        
        print('ERROR TOMTOM URL: %s' % url, file=sys.stderr)
        print('ERROR TOMTOM: %s' % error, file=sys.stderr)
        
        return []

    if 'error' in response['calculateRouteResponse'].keys():
        return []

    if parseData:
        return parse(response)
    return response
