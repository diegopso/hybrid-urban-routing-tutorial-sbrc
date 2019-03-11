import math
from smaframework.tool.constants import miles2km

'''
 * Evaluate the perceived time of travel.
 * Based on Paper: Abrantes, P. A. L., & Wardman, M. R. (2011). Meta-analysis of UK values of travel time: An update. Transportation Research Part A: Policy and Practice, 45(1), 1–17. https://doi.org/10.1016/J.TRA.2010.08.003
 *
 * @param i - the step counter
 * @param s - the step data
'''
def perceived_time(i, s):
    if s['travel_mode'] == 'TAXI':
        if 'congested_time' in s.keys():
            score = (s['duration'] - s['congested_time']) + s['congested_time'] * 1.54 + s['wait'] * 1.7
        else:
            score = s['duration'] + s['wait'] * 1.7
    elif s['travel_mode'] == 'WALKING':
        # Based on Book Chapter: Ch5 Pg 5-11 - Transit Capacity and Quality of Service Manual, 3rd ed. describing predisposition to walk for reaching a rapid-transit mode
        distance = (s['distance'] / 2) if s['phase'] == 'access' and s['next_mode'] in ['SUBWAY'] else s['distance']
        score = s['duration'] * (1.65 / walkability(distance))
    else:
        score = s['duration'] * 0.78 + s['wait'] * 1.7

    return score

'''
 * Based on Report: Cycling and Walking: the grease in our mobility chain. Pg. 21 - https://www.researchgate.net/publication/311773579_Cycling_and_walking_the_grease_in_our_mobility_chain
 * Interpolate curve for Acceptable walking distance between parking place and store using `fit cubic {0,1}, {290,0.8}, {435,0.6}, {520,0.4}, {675,0.2}, {1000,0}` on WolframAlpha.
 * Note: all trips purpouse, not only transit access.
 *
 * @param x - the walking distance in meters.
'''
def walking_acceptability(x):
    y = 2.78409e-9 * pow(x, 3) - 4.03692e-6 * pow(x, 2) - 2.53449e-4 * x + 1.00038
    return y if y > 0.01 else 0.01 # ensure non-zero division

'''
 * Based on Paper: Yang, Y., & Diez-Roux, A. V. (2012). Walking Distance by Trip Purpose and Population Subgroups. American Journal of Preventive Medicine, 43(1), 11–19. https://doi.org/10.1016/J.AMEPRE.2012.03.015
 * Note: all trips purpouse, not only transit access.
 *
 * @param x - the walking distance in meters.
'''
def walking_distance_decay(x):
    x = x / 1000 / miles2km # meters to miles
    y = 0.98 * math.exp(-1.71 * x)

    return y if y > 0.01 else 0.01 # ensure non-zero division

'''
 * Based on Book Chapter: Ch4 Pg 4-18 - Transit Capacity and Quality of Service Manual, 3rd ed. https://www.researchgate.net/publication/293811979_Transit_Capacity_and_Quality_of_Service_Manual_3rd_ed
 * Interpolate curve for Washignton DC (low income) using `interpolating polynomial {0,1},{0.075,0.75},{0.15,0.5},{0.25,0.25},{0.45, 0}` or `fit exponential {0,1},{0.075,0.75},{0.15,0.5},{0.25,0.25},{0.45, 0}` on WolframAlpha.
 * Note: specific for transit access.
 *
 * @param x - the walking distance in meters.
'''
def walkability(x, mode='exponential'):
    x = x / 1000 / miles2km # meters to miles
    
    if mode == 'exponential':
        y = 1.0388 * math.exp(-5.36561 * x)
    else:
        y = -45.8554 * pow(x, 4) + 40.8289 * pow(x, 3) - 7.38095 * pow(x, 2) - 2.99008 * x + 1

    return y if y > 0.01 else 0.01 # ensure non-zero division

'''
 * Reshaped curve from Based on Book Chapter: Ch4 Pg 4-18 - Transit Capacity and Quality of Service Manual, 3rd ed. https://www.researchgate.net/publication/293811979_Transit_Capacity_and_Quality_of_Service_Manual_3rd_ed
 * Fit curve for Washignton DC (low income) using `fit exponential {0,1},{0.075,0.75},{0.15,0.5},{0.25,0.25},{0.45, 0}` on WolframAlpha.
 * Use lamda and reshape the curve to start close to 0 and raise.
 * Note: specific for transit access.
 *
 * @param x - the walking distance in meters.
'''
def walking_monetary_impact(x):
    x = x / 1000 # meters to km    
    y = 0.001 * math.exp(5.36561 * x)
    return y if y > 0.01 else 0.01 # ensure non-zero division

'''
 * Evaluate the perceived price per minute of trip.
 *
 * Uber price per minute extracted froom: https://www.ridesharingdriver.com/how-much-does-uber-cost-uber-fare-estimator/
 *             Cost/Mile   Cost/Minute Base Fare   Booking Fee     Minimum Fare
 * UberX       $0.90       $0.15       $0          $2.10           $5.60
 * UberPool    $0.85       $0.11       $0          $2.10           $5.60
 * UberXL      $1.55       $0.30       $1          $2.35           $8.35
 * UberSelect  $2.35       $0.40       $5          $2.35           $11.65
 * UberBlack   $3.55       $0.45       $8          n/a             $15
 * UberSUV     $4.25       $0.55       $15         n/a             $25
 * 
 * TODO: evaluate price per minute on transit from real data.
 *
 * @param i - the step counter
 * @param s - the step data
'''
def percived_price_per_minute(i, s):
    # normalizer = 0
    # if s['travel_mode'] == 'TAXI':
    #     normalizer = 0.15
    # else:
    #     normalizer = 0.05 # TODO: evaluate from real data

    from haversine import haversine

    price = s['price'] if s['price'] > 0 else 2.5
    duration = s['duration'] / 60
    distance = haversine(tuple(s['origin']), tuple(s['destination']))

    if distance == 0:
        normalizer = float('inf')
    else:
        normalizer = duration * price / distance

    # print(i, s['travel_mode'], duration, distance, price, normalizer)

    return perceived_time(i, s) * normalizer

def perceived_score(coef, i, s):
    score = coef * s['price'] + (1-coef) * percived_price_per_minute(i, s)
    # print(i, s['travel_mode'], percived_price_per_minute(i, s), s['price'], score)
    return score