import os
import numpy as np
import pandas as pd
import time
import datetime as dt
import uuid as IdGenerator

import multiprocessing as mp
import urllib.request
import json

def extract_url(params):
    (app_id, app_code, departure, arrival, date, modes_str) = params

    query = {
        "app_id": app_id,
        "app_code": app_code,
        "dep": '%f,%f' % departure,
        "arr": '%f,%f' % arrival,
        "time": "%s" % date.isoformat(),
        "routing": modes_str
    }

    url = 'https://transit.cit.api.here.com/v3/route.json?' + urllib.parse.urlencode(query)
    response = urllib.request.urlopen(url).read().decode("utf-8")

    print(response)

    response = json.loads(response)

def extract(app_id, app_code, departures, arrivals, dates, modes_str, **kwargs):
    multiprocess = 'pool_size' in kwargs.keys() and int(kwargs['pool_size']) > 1
    if multiprocess:
        pool_size = int(kwargs['pool_size'])
        pool = mp.Pool(pool_size)
        pool.map(extract_url, [(app_id, app_code, departures[i], arrivals[i], dates[i], modes_str) for i in range(0, len(departures))])
        pool.close()
        pool.join()
    else:
        for i in range(0, len(departures)):
            extract_url((app_id, app_code, departures[i], arrivals[i], dates[i], modes_str))