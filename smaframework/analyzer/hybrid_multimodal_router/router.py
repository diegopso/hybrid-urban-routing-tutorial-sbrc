from smaframework.common.address_keywords_extension_map import parse_str as parse_address_str
import smaframework.extractor.here.traffic as HereTrafficExtractor
import smaframework.extractor.google.directions as GoogleDirectionsExtractor
import smaframework.extractor.uber as UberExtractor
import numpy as np
import networkx as nx
import haversine
import time

def analyse_ways(app_id, app_code, ways):
    result = []
    for way in ways:
        routes = []
        for route in way:
            route = analyse_route(app_id, app_code, route)
            routes.append(route)
        result.append(routes)
    return result

def analyse_route(app_id, app_code, route):
    resulting_route = []
    for step in route:
        if step['distance'] > 500:
            observations = int(step['distance'] / 500) + 1
            for i in range(observations, 0, -1):
                origin = list(step['origin'])
                destination = list(step['destination'])

                destination = [abs(origin[0] - destination[0])/i + origin[0], abs(origin[1] - destination[1])/i + origin[1]]

                step_ = step.copy()
                step_['origin'] = origin
                step_['destination'] = destination
                step_['duration'] = step['duration'] / i
                step_['distance'] = step['distance'] / i
                
                o = [(destination[0] + origin[0])/2, (destination[1] + origin[1])/2]
                traffic = HereTrafficExtractor.lat_lon_zoom(app_id, app_code, o[0], o[1])
                step_['traffic'] = match_data(step['address_keywords'], traffic, True)
                
                resulting_route.append(step_)

                step['duration'] = step['duration'] - step_['duration']
                step['distance'] = step['distance'] - step_['distance']
                step['origin'] = destination

                origin = destination
        else:
            o = [(step['destination'][0] + step['origin'][0])/2, (step['destination'][1] + step['origin'][1])/2]
            traffic = HereTrafficExtractor.lat_lon_zoom(app_id, app_code, o[0], o[1])
            step['traffic'] = match_data(step['address_keywords'], traffic, True)
            resulting_route.append(step)
            
    return resulting_route

def match_data(keywords, traffic, summary=False):
    max_score = -1
    best_observations = []
    for k, observation in traffic['data'].items():
        kws = parse_address_str(observation['DE'])
        score = len(set(keywords).intersection(kws))

        if score == max_score:
            best_observations.append(observation)
        elif score > max_score:
            max_score = score
            best_observations = [observation]

    if not summary:
        return best_observations
    
    jfp = [o['JF'] for o in best_observations if o['QD'] == '+']
    jfp = np.mean(jfp) if len(jfp) > 0 else 0
    jfn = [o['JF'] for o in best_observations if o['QD'] == '-']
    jfn = np.mean(jfn) if len(jfn) > 0 else 0
    jf = max(jfp, jfn)

    cnp = [o['CN'] for o in best_observations if o['QD'] == '+']
    cnp = np.mean(cnp) if len(cnp) > 0 else 0
    cnn = [o['CN'] for o in best_observations if o['QD'] == '-']
    cnn = np.mean(cnn) if len(cnn) > 0 else 0
    cn = max(cnp, cnn)

    return {'JF': jf, 'CN': cn}

def merge_segments(driving_ways, **kwargs):
    config = {}
    config.update({'thershold': 5})
    config.update(kwargs)

    result = []
    for way in driving_ways:
        routes = []
        for route in way:
            route = merge_route_segments(route, config['thershold'])
            routes.append(route)
        result.append(routes)

    return result

def merge_route_segments(route, thershold=5, merge=False):
    result = []
    previous = {'class': ''}
    pointer = None
    first_critical = None
    last_critical = None
    for step in route:
        clazz = 'critical' if step['traffic']['JF'] >= thershold else 'non-critical'
        s = None
        if clazz == previous['class']:
            s = {
                'origin': previous['origin'],
                'destination': step['destination'],
                'duration': step['duration'] + previous['duration'],
                'distance': step['distance'] + previous['distance'],
                'class': clazz
            }

            result[pointer] = s
        else:
            s = {
                'origin': step['origin'],
                'destination': step['destination'],
                'duration': step['duration'],
                'distance': step['distance'],
                'class': clazz
            }

            result.append(s)
            pointer = len(result) - 1

        if clazz == 'critical':
            if first_critical == None:
                first_critical = pointer
            last_critical = pointer

        previous = s

    if not merge or first_critical == last_critical:
        return result

    # merge intermediary regions
    duration = 0
    distance = 0
    for i in range(first_critical, last_critical):
        duration = duration + result[i]['duration']
        distance = distance + result[i]['distance']

    s = {
        'origin': result[first_critical]['origin'],
        'destination': result[last_critical]['destination'],
        'duration': duration,
        'distance': distance,
        'class': 'critical'
    }

    result[first_critical : last_critical+1] = [s]

    return result

def select_best_transit_route(trip, score_function):
    selected = None
    min_cost = float("inf")

    for route in trip:
        cost = 0
        for (i, step) in enumerate(route):
            cost = cost + score_function(i, step)
    
        if cost < min_cost:
            min_cost = cost
            selected = route

    return selected

'''
 * Get the available options to replace a set of trips from the same source to the same sink position.
 * 
 * @param access_keys - The keys to access Uber and GoogleMaps APIs.
 * @param trips - The set of trips to be evaluated.
'''
def get_available_options(access_keys, trips, **kwargs):
    config = {
        "uber_modality": 'uberX',
        "prices": {
            "TRANSIT": 2.50,
            "WALKING": 0,
        },
        'score_function': lambda i, s: s['duration']
    }
    config.update(kwargs)
    
    result = []
    for driving_ways in trips:
        result.append(get_trip_available_options(access_keys, driving_ways, **config))

    return result 

def _capsule(params):
    (fn, params) = params

    kwargs = {}
    if isinstance(params[-1], dict):
        kwargs = params[-1]
        del params[-1]

    return fn(*params, **kwargs)

'''
 * Get the available options to replace a set of driving ways from the same source to the same sink position.
 * 
 * PSEUDO-CODE:
 *
 * def get_hybrid_route(origin, destination):
 *    driving_way <- get_driving_way(origin, destination) # retrieves driving path using Google Directions
 *    transit_start_candidates <- new list()
 *    transit_end_candidates <- new list()
 *    options <- new list()
 *
 *    foreach (index, step) in driving_way.steps:
 *        if step.length > 500:
 *            fragments <- split_step(step, 500) # split the step in 500m chunks
 *            splice(driving_way.steps, index, 1, fragments) # replace 1 position from the index with the specified list
 *            continue
 *
 *        traffic = get_traffic_data(step.origin, step.destination) # consult the traffic data from HERE in the middle position between origin and destination, also performs the address-GPS matching using USPS address abreviation dataset
 *        if is_congested(traffic):
 *            append(transit_start_candidates, step.origin)
 *            append(transit_end_candidates, step.destination)
 *
 *    foreach (index, ts) in transit_start_candidates:
 *        option <- get_hpv_started_option(origin, ts, destination) # gets a HPV route from origin to TS and a transit route from TS to destination
 *        append(options, option)
 *
 *    foreach (index, te) in transit_end_candidates:
 *        option <- get_hpv_started_option(origin, te, destination) # gets a transit route from origin to TE and a HPV route from TE to destination
 *        append(options, option)
 *
 *    foreach (index, ts) in transit_start_candidates:
 *        foreach (jindex, te) in transit_end_candidates:
 *            mixed_options <- get_mixed_option(origin, ts, te, destination) # gets four options where origin to TS is made by HPV or WALK, TE to destination is made by HPV or WALK and TS to TE is made by transit
 *            concat(options, mixed_options) # join lists
 *
 *    return options
 *
 * @param access_keys - The keys to access Uber and GoogleMaps APIs.
 * @param driving_ways - The set of driving ways to be evaluated.
'''
def get_trip_available_options(access_keys, driving_ways, **config):
    start = driving_ways[0][0]['origin']
    end = driving_ways[0][-1]['destination']

    congested_times = []
    for driving_way in driving_ways:
        congested_time = 0
        for segment in driving_way:
            if segment['class'] == 'critical':
                congested_time = congested_time + segment['duration']
        congested_times.append(congested_time)

    transit_starts = []
    transit_ends = []
    congested_time = 0
    for (i, driving_way) in enumerate(driving_ways):
        for segment in driving_way:
            if segment['class'] == 'critical':
                transit_starts.append({'position': segment['origin'], 'traffic': congested_time})
                transit_ends.append({'position': segment['destination'], 'traffic': congested_times[i] - congested_time})
                congested_time = congested_time + segment['duration']

    options = get_taxi_started_options(access_keys, transit_starts, start, end, config)
    options.extend(get_taxi_ended_options(access_keys, transit_ends, start, end, config))
    options.extend(get_skip_traffic_only_options(access_keys, start, end, transit_starts, transit_ends, config))

    full_taxi_trip = UberExtractor.estimate(access_keys['UBER_SERVER_TOKEN'], start, end, 1, config['uber_modality'])
    full_taxi_step = [{
            "address_keywords": [],
            "duration": full_taxi_trip['duration'],
            "congested_time": congested_time,
            "wait": full_taxi_trip['wait'],
            "travel_mode": "TAXI",
            "vehicle_type": config['uber_modality'],
            "origin": full_taxi_trip['origin'],
            "distance": full_taxi_trip['distance'],
            "destination": full_taxi_trip['destination'],
            "price":  full_taxi_trip['price']
        }]

    full_transit_trip = GoogleDirectionsExtractor.extract_single(access_keys['GOOGLE_MAPS_KEY'], start, end, int(time.time()), 'transit', config['prices'])
    full_transit_trip = select_best_transit_route(full_transit_trip, config['score_function'])

    options.append(full_taxi_step)
    options.append(full_transit_trip)

    return options

'''
 * Choose one of the given options based on the score function minimization.
 * 
 * @param options - The given options
 * @param score_function - The function to evaluate the score of a step in the option. Receives as a params:
 *      * i - the step counter
 *      * data - the step data (price, duration, distance, mode, vehicle, origin, end)
'''
def choose(options, score_function):
    min_score = float("inf")
    for option in options:
        current_score = 0
        for i, step in enumerate(option):
            current_score = current_score + score_function(i, step)

        if current_score < min_score:
            min_score = current_score
            selected = option

    return selected

def get_skip_traffic_only_options(access_keys, start, end, transit_starts, transit_ends, config):
    options = []
    for ts in transit_starts:
        for te in transit_ends:
            transit_route = GoogleDirectionsExtractor.extract_single(access_keys['GOOGLE_MAPS_KEY'], ts['position'], te['position'], int(time.time()), 'transit', config['prices'])
            transit_route = select_best_transit_route([summarize_steps(steps) for steps in transit_route], config['score_function'])
            
            end_step = transit_route[-1]
            start_step = transit_route[0]
            length = len(transit_route)

            if transit_route[-1]['travel_mode'] == 'WALKING':
                end_nearest_stop = transit_route[-1]['origin']
                if length > 1:
                    del transit_route[-1]
            else:
                end_nearest_stop = transit_route[-1]['destination']
    
            if transit_route[0]['travel_mode'] == 'WALKING':
                start_nearest_stop = transit_route[0]['destination']
                if length > 1:
                    del transit_route[0]
            else:
                start_nearest_stop = transit_route[0]['origin']

            if length == 1 and transit_route[0]['travel_mode'] == 'WALKING':
                end_nearest_stop = start_nearest_stop
                del transit_route[0]

            start_taxi_segment = UberExtractor.estimate(access_keys['UBER_SERVER_TOKEN'], start, start_nearest_stop, 1, config['uber_modality'])
            start_taxi_step = [{
                            "address_keywords": [],
                            "duration": start_taxi_segment['duration'],
                            "congested_time": ts['traffic'],
                            "wait": start_taxi_segment['wait'],
                            "travel_mode": "TAXI",
                            "vehicle_type": config['uber_modality'],
                            "origin": start_taxi_segment['origin'],
                            "distance": start_taxi_segment['distance'],
                            "destination": start_taxi_segment['destination'],
                            "price":  start_taxi_segment['price']
                        }]
            
            start_walk_segment = GoogleDirectionsExtractor.extract_single(access_keys['GOOGLE_MAPS_KEY'], start, start_nearest_stop, int(time.time()), 'walking', config['prices'])
            start_walk_step = select_best_transit_route([summarize_steps(s) for s in start_walk_segment], config['score_function'])

            end_taxi_segment = UberExtractor.estimate(access_keys['UBER_SERVER_TOKEN'], end_nearest_stop, end, 1, config['uber_modality'])
            end_taxi_step = [{
                            "address_keywords": [],
                            "duration": end_taxi_segment['duration'],
                            "congested_time": te['traffic'],
                            "wait": end_taxi_segment['wait'],
                            "travel_mode": "TAXI",
                            "vehicle_type": config['uber_modality'],
                            "origin": end_taxi_segment['origin'],
                            "distance": end_taxi_segment['distance'],
                            "destination": end_taxi_segment['destination'],
                            "price":  end_taxi_segment['price']
                        }]

            end_walk_segment = GoogleDirectionsExtractor.extract_single(access_keys['GOOGLE_MAPS_KEY'], end_nearest_stop, end, int(time.time()), 'walking', config['prices'])
            end_walk_step = select_best_transit_route([summarize_steps(s) for s in end_walk_segment], config['score_function'])
            
            if start_walk_step and transit_route and end_walk_step:
                options.append(summarize_steps(start_walk_step + transit_route + end_walk_step))

            if start_walk_step and transit_route and end_taxi_step:
                options.append(start_walk_step + transit_route + end_taxi_step)

            if start_taxi_step and transit_route and end_walk_step:
                options.append(start_taxi_step + transit_route + end_walk_step)
            
            route = start_taxi_step + transit_route + end_taxi_step
            if len(route) > 2:
                options.append(route)

    return options

def summarize_steps(steps):
    result = []
    current = {'travel_mode': '', 'phase': ''}
    for (i, step) in enumerate(steps):
        if current['travel_mode'] != step['travel_mode']:
            if current['travel_mode'] != '':
                result.append(current)

            if current['phase'] == 'access' and step['travel_mode'] == 'TRANSIT':
                current['next_mode'] = step['vehicle_type']

            current = {
                "address_keywords": [],
                "duration": 0,
                "wait": 0,
                "travel_mode": step['travel_mode'],
                "vehicle_type": step['vehicle_type'],
                "origin": steps[0]['origin'],
                "distance": 0,
                "price": 0,
                "destination": steps[-1]['destination'],
            }

        current['duration'] = current['duration'] + step['duration']
        current['distance'] = current['distance'] + step['distance']
        current['price'] = current['price'] + step['price']
        current['wait'] = current['wait'] + step['wait']
        current['phase'] = 'headway' if step['travel_mode'] != 'WALKING' else ('egress' if i == len(steps) - 1 else 'access')

    if current['travel_mode'] != '':
        result.append(current)

    return result

def get_taxi_ended_options(access_keys, transit_ends, start, end, config):
    exchanges = len(transit_ends)
    transit_trip_segments = GoogleDirectionsExtractor.extract(access_keys['GOOGLE_MAPS_KEY'], [start] * exchanges, [te['position'] for te in transit_ends], [int(time.time())] * exchanges, 'transit', config['prices'])
    
    transit_ends_nearest_stops = []
    for i in range(0, exchanges):
        route = select_best_transit_route(transit_trip_segments[i], config['score_function'])
        if route[-1]['travel_mode'] == 'WALKING':
            transit_ends_nearest_stops.append(route[-1]['origin'])
        else:
            transit_ends_nearest_stops.append(route[-1]['destination'])

        transit_trip_segments[i] = route

    taxi_trip_segments = UberExtractor.extract(access_keys['UBER_SERVER_TOKEN'], transit_ends_nearest_stops, [end] * exchanges, [1]*exchanges, config['uber_modality'])

    for i in range(0, exchanges):
        if transit_trip_segments[i][-1]['travel_mode'] == 'WALKING':
            del transit_trip_segments[i][-1]
        
        taxi_step = {
            "address_keywords": [],
            "duration": taxi_trip_segments[i]['duration'],
            "congested_time": transit_ends[i]['traffic'],
            "wait": taxi_trip_segments[i]['wait'],
            "travel_mode": "TAXI",
            "vehicle_type": config['uber_modality'],
            "origin": taxi_trip_segments[i]['origin'],
            "distance": taxi_trip_segments[i]['distance'],
            "destination": taxi_trip_segments[i]['destination'],
            "price":  taxi_trip_segments[i]['price']
        }

        transit_trip_segments[i].append(taxi_step)

    return transit_trip_segments

def get_taxi_started_options(access_keys, transit_starts, start, end, config):
    exchanges = len(transit_starts)
    transit_trip_segments = GoogleDirectionsExtractor.extract(access_keys['GOOGLE_MAPS_KEY'], [ts['position'] for ts in transit_starts], [end] * exchanges, [int(time.time())] * exchanges, 'transit', config['prices'])

    transit_starts_nearest_stops = []
    for i in range(0, exchanges):
        route = select_best_transit_route(transit_trip_segments[i], config['score_function'])
        if route[0]['travel_mode'] == 'WALKING':
            transit_starts_nearest_stops.append(route[0]['destination'])
        else:
            transit_starts_nearest_stops.append(route[0]['origin'])

        transit_trip_segments[i] = route
    
    taxi_trip_segments = UberExtractor.extract(access_keys['UBER_SERVER_TOKEN'], [start] * exchanges, transit_starts_nearest_stops, [1]*exchanges, config['uber_modality'])

    for i in range(0, exchanges):
        if transit_trip_segments[i][0]['travel_mode'] == 'WALKING':
            transit_trip_segments[i] = transit_trip_segments[i][1:]
        
        taxi_step = {
            "address_keywords": [],
            "duration": taxi_trip_segments[i]['duration'],
            "wait": taxi_trip_segments[i]['wait'],
            "congested_time": transit_starts[i]['traffic'],
            "travel_mode": "TAXI",
            "vehicle_type": config['uber_modality'],
            "origin": taxi_trip_segments[i]['origin'],
            "distance": taxi_trip_segments[i]['distance'],
            "destination": taxi_trip_segments[i]['destination'],
            "price":  taxi_trip_segments[i]['price']
        }

        transit_trip_segments[i].insert(0, taxi_step)

    return transit_trip_segments