import tweepy, time, datetime, logging, os
from tweepy import OAuthHandler
import pandas as pd

import pyproj    
import shapely
import shapely.ops as ops
from shapely.geometry.polygon import Polygon as ShapelyPolygon
from functools import partial

"""
    Twitter data extarctor. The available data collected by tweepy in the current version of the API  is:
    [
        '_api',
        '_json',
        'created_at',
        'id',
        'id_str',
        'text',
        'truncated',
        'entities',
        'metadata',
        'source',
        'source_url',
        'in_reply_to_status_id', 
        'in_reply_to_status_id_str',
        'in_reply_to_user_id',
        'in_reply_to_user_id_str',
        'in_reply_to_screen_name',
        'author',
        'user',
        'geo',
        'coordinates',
        'place',
        'contributors',
        'retweeted_status',
        'is_quote_status',
        'retweet_count',
        'favorite_count',
        'favorited',
        'retweeted',
        'lang'
    ]
"""

def extract(access, geocode, layer='twitter', **kwargs):
    if 'consumer_key' in access.keys() and 'consumer_secret' in access.keys() and 'access_token' in access.keys() and 'access_secret' in access.keys():
        auth = OAuthHandler(access['consumer_key'], access['consumer_secret'])
        auth.set_access_token(access['access_token'], access['access_secret'])
    
    if 'items_per_request' not in kwargs.keys():
        kwargs['items_per_request'] = 100

    if 'wait' not in kwargs.keys():
        kwargs['wait'] = 60

    if 'max_area' not in kwargs.keys():
        kwargs['max_area'] = 200

    api = tweepy.API(auth)
    count = 0
    total = 0
    data = []

    while True:
        try:
            collection = tweepy.Cursor(api.search, geocode=geocode).items(kwargs['items_per_request'])
            for status in collection:
                if status.coordinates:
                    data.append((status.user.screen_name + ':' + str(status.user.id), int(time.mktime(status.created_at.timetuple())), status.coordinates['coordinates'][1], status.coordinates['coordinates'][0], layer))
                    total = total + 1
                elif status.place:
                    polygon = ShapelyPolygon(status.place.bounding_box.coordinates[0])
                    geom_area = ops.transform(
                        partial(
                            pyproj.transform,
                            pyproj.Proj(init='EPSG:4326'),
                            pyproj.Proj(
                                proj='aea',
                                lat1=polygon.bounds[1],
                                lat2=polygon.bounds[3])),
                        polygon)

                    if geom_area.area < kwargs['max_area']:
                        point = polygon.representative_point().xy
                        data.append((status.user.screen_name + ':' + str(status.user.id), int(time.mktime(status.created_at.timetuple())), point[1][0], point[0][0], layer))
        # except tweepy.TweepError as e:
        except:
            logpath = 'data/logs/'
            if not os.path.exists(logpath):
                os.makedirs(logpath)

            filename = datetime.datetime.fromtimestamp(time.time()).strftime(logpath + 'error_%Y%m%d.log')
            logging.basicConfig(filename=filename, filemode='a+')
            logging.exception("message")
            
            if 'force' in kwargs.keys() and kwargs['force']:
                time.sleep(5 * kwargs['wait'])
                continue
            
        if len(data) > 0:
            frame = pd.DataFrame(data)
            frame.columns = ['uid', 'timestamp', 'lat', 'lon', 'layer']
            data = []

            if 'filename' not in kwargs.keys():
                kwargs['filename'] = 'data/entries/twitter.csv'

            if not os.path.exists('data/entries/'):
                os.makedirs('data/entries/')

            header = not os.path.exists(kwargs['filename'])
            with open(kwargs['filename'], 'a+') as file:
                frame.to_csv(file, header=header, index=False)

        count = count + 1
        if 'limit' in kwargs.keys() and count >= kwargs['limit']:
            break
        time.sleep(kwargs['wait'])
