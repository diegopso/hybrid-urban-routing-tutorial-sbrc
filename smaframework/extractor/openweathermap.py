import urllib, json, uuid, os, datetime, time, logging, random, math, traceback
import pandas as pd
from shapely.geometry import Point

def extract(access, region, layer='openweathermap', **kwargs):
    if 'samples' not in kwargs.keys():
        kwargs['samples'] = 3

    if 'wait' not in kwargs.keys():
        kwargs['wait'] = 3600

    if 'api_version' not in kwargs.keys():
        kwargs['api_version'] = '2.5'

    if 'force' not in kwargs.keys():
        kwargs['force'] = True

    count = 0
    minx, miny, maxx, maxy = region.bounds
    
    while True:
        try:
            point = Point(random.uniform(minx, maxx), random.uniform(miny, maxy))
            while not region.contains(point):
                point = Point(random.uniform(minx, maxx), random.uniform(miny, maxy))
            
            point = point.xy
            content = urllib.request.urlopen("http://api.openweathermap.org/data/%s/weather?lat=%f&lon=%f&appid=%s" % (kwargs['api_version'], point[1][0], point[0][0], access)).read().decode('utf-8')
            d = json.loads(content)

            if 'filename' not in kwargs.keys():
                kwargs['filename'] = 'data/entries/openweathermap.csv'

            if not os.path.exists('data/entries/'):
                os.makedirs('data/entries/')

            header = not os.path.exists(kwargs['filename'])
            with open(kwargs['filename'], 'a+') as file:
                if header:
                    file.write(",".join([
                        'uid',
                        'timestamp',
                        'lat',
                        'lon',
                        'layer',
                        'weather',
                        'base',
                        'temperature',
                        'pressure',
                        'humidity',
                        'temp_min',
                        'temp_max',
                        'sea_level',
                        'grnd_level',
                        'wind_speed',
                        'wind_deg',
                        'cloudiness',
                        'sunrise',
                        'sunset',
                        'city_name'
                        ]) + "\n")

                name = None
                if 'district' in kwargs.keys():
                    name = kwargs['district'].encode('utf-8')
                elif 'name' in d.keys():
                    name = d['name'].encode('utf-8')

                data = (
                    'openweathermap_%s' % uuid.uuid4().hex,
                    d['dt'] or math.floor(time.time()),
                    d['coord']['lat'] if 'coord' in d.keys() and 'lat' in d['coord'].keys() else None,
                    d['coord']['lon'] if 'coord' in d.keys() and 'lon' in d['coord'].keys() else None,
                    layer,
                    d['weather'][0]['description'] if 'weather' in d.keys() and len(d['weather']) > 0 else None,
                    d['base'] if 'base' in d.keys() else None,
                    d['main']['temp'] if 'main' in d.keys() and 'temp' in d['main'].keys() else None,
                    d['main']['pressure'] if 'main' in d.keys() and 'pressure' in d['main'].keys() else None,
                    d['main']['humidity'] if 'main' in d.keys() and 'humidity' in d['main'].keys() else None,
                    d['main']['temp_min'] if 'main' in d.keys() and 'temp_min' in d['main'].keys() else None,
                    d['main']['temp_max'] if 'main' in d.keys() and 'temp_max' in d['main'].keys() else None,
                    d['main']['sea_level'] if 'main' in d.keys() and 'sea_level' in d['main'].keys() else None,
                    d['main']['grnd_level'] if 'main' in d.keys() and 'grnd_level' in d['main'].keys() else None,
                    d['wind']['speed'] if 'wind' in d.keys() and 'speed' in d['wind'].keys() else None,
                    d['wind']['deg'] if 'wind' in d.keys() and 'deg' in d['wind'].keys() else None,
                    d['clouds']['all'] if 'clouds' in d.keys() and 'all' in d['clouds'].keys() else None,
                    d['sys']['sunrise'] if 'sys' in d.keys() and 'sunrise' in d['sys'].keys() else None,
                    d['sys']['sunset'] if 'sys' in d.keys() and 'sunset' in d['sys'].keys() else None,
                    name
                    )

                file.write(",".join(map(lambda n: str(n), data)) + "\n")
            
            count = count + 1
            if 'limit' in kwargs.keys() and count >= kwargs['limit']:
                break

            time.sleep(math.flor(kwargs['wait'] / kwargs['samples']))
        except:
            if 'force' in kwargs.keys() and kwargs['force']:
                logpath = 'data/logs/'
                if not os.path.exists(logpath):
                    os.makedirs(logpath)

                filename = datetime.datetime.fromtimestamp(time.time()).strftime(logpath + 'error_%Y%m%d.log')
                logging.basicConfig(filename=filename, filemode='a+')
                logging.exception("message")

                time.sleep(kwargs['wait'])
                continue
            else:
                traceback.print_exc()
                return