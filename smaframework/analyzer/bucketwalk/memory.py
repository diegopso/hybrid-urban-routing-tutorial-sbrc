from haversine import haversine
import itertools

def closest(index, point, dist=None, radius=1):
    if dist == None:
        dist = haversine

    key = hash_sample(point, index['hashing_dist'], index['origin'])
    cube = get_cube(index, key, radius)

    min_distance = float("inf")
    closest = None
    for i in cube:
        distance = dist(point, index['points'][i])
        if distance < min_distance:
            min_distance = distance
            closest = i

    return (closest, min_distance)

def get_cube(index, key, radius):
    ranges = map(lambda k: list(range(k - radius, k + radius + 1)), key)

    keys = list(itertools.product(*ranges))

    keys = map(lambda key: '-'.join([str(k) for k in key]), keys)

    result = []
    for k in keys:
        if k not in index.keys():
            continue
        result.extend(index[k])

    return result

def hash_sample(point, hashing_dist, origin):
    if isinstance(hashing_dist, list):
        return [int((point[i]-origin[i]) / hashing_dist[i]) for i in range(0, len(point))]
    return [int((point[i]-origin[i]) / hashing_dist) for i in range(0, len(point))]

def in_memory(points, hashing_dist=0.005, origin=None):
    if len(points) == 0:
        return None

    dimension = len(points[0])

    if origin == None:
        origin = (0,) * dimension

    keys = list(map(lambda point: hash_sample(point, hashing_dist, origin), points))
    index = {
        "hashing_dist": hashing_dist,
        "dimension": dimension,
        "origin": origin,
        "points": points
    }

    for i in range(0, len(keys)):
        key = '-'.join([str(k) for k in keys[i]])
        if key not in index.keys():
            index[key] = []
        index[key].append(i)

    return index