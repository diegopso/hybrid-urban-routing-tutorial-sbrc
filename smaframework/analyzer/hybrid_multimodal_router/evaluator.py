import numpy as np
import pandas as pd
from haversine import haversine
import smaframework.analyzer.hybrid_multimodal_router.model as Model

def evaluate(trips, routes, group_id, profile=1):
    frames = []

    for route in routes:
        if not isinstance(route, dict) or len(route['options']) == 0:
            continue

        metadatas = []
        for option in route['options'][0]:
            if not option:
                continue

            metadata = extract_metadata(option)
            metadatas.append(metadata)

        metadatas = pd.DataFrame(metadatas)
        norm = metadatas[['duration', 'wait', 'congested_time', 'perceived_duration', 'cost', 'traversed_distance', 'walking_distance']].apply(lambda x: x / np.max(x))


        metadatas = metadatas.join(norm, rsuffix='_norm')

        metadatas['effective_cost'] = metadatas['duration'] * metadatas['cost']
        metadatas['effective_cost_perceived'] = metadatas['perceived_duration'] * metadatas['cost']
        
        metadatas['effective_cost_norm'] = metadatas['duration_norm'] * metadatas['cost_norm']
        metadatas['effective_cost_perceived_norm'] = metadatas['perceived_duration_norm'] * metadatas['cost_norm']
        
        metadatas['weight'] = trips[route['index']]['weight']
        metadatas['origin_lat'] = trips[route['index']]['link'][0]['lat']
        metadatas['origin_lon'] = trips[route['index']]['link'][0]['lng']
        metadatas['destination_lat'] = trips[route['index']]['link'][1]['lat']
        metadatas['destination_lon'] = trips[route['index']]['link'][1]['lng']
        metadatas['group_id'] = group_id
        
        distance = haversine(tuple(route['options'][0][0][0]['origin']), tuple(route['options'][0][0][-1]['destination']))
        metadatas['distance'] = distance

        frames.append(metadatas)

    if len(frames) == 0:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)

'''
    TAXI - only TAXI
    TRANSIT - no TAXI
    HYBRID - has TAXI and OTHER
    WALKING - only WALKING
'''
def select_category(modes):
    modes = list(set(modes))

    if len(modes) == 1:
        if modes[0] == 'uberX':
            return 'TAXI'
        elif modes[0] == 'WALKING':
            return 'WALKING'
        return 'TRANSIT'

    return 'HYBRID' if 'uberX' in modes else 'TRANSIT'

def extract_metadata(option):
    traversed_distance = 0
    duration = 0
    perceived_duration = 0
    cost = 0
    walking_distance = 0
    modes = []
    congested_time = 0
    wait = 0

    for (i, step) in enumerate(option):
        traversed_distance = traversed_distance + abs(step['distance'])
        duration = duration + abs(step['duration'])
        cost = cost + abs(step['price'])
        wait = wait + abs(step['wait'])
        modes.append(step['vehicle_type'] if step['travel_mode'] in ['TRANSIT', 'TAXI'] else step['travel_mode'])
        
        if step['travel_mode'] == 'WALKING':
            walking_distance = walking_distance + abs(step['distance'])

        if 'congested_time' in step.keys():
            congested_time = congested_time + abs(step['congested_time'])
        
        perceived_duration = perceived_duration + abs(Model.perceived_time(i, step))

    return {
        'traversed_distance': traversed_distance,
        'duration': duration,
        'perceived_duration': perceived_duration,
        'cost': cost,
        'walking_distance': walking_distance,
        'modes': modes,
        'category': select_category(modes),
        'congested_time': congested_time,
        'wait': wait,
    }