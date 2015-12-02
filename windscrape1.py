__author__ = 'crimzoid'

import requests
from bs4 import BeautifulSoup
from dateutil import parser
import re
import time
import datetime
import pandas as pd
import sys

reload(sys)
sys.setdefaultencoding("utf-8")

params = ['Sonnenutergang', 'Temperatur', 'Wind',
          'Rain', 'Hum', 'Pressure', 'Predicted']
iframe = 'http://www.surf-und-segelschule-mueggelsee.de/weather/wetter.php'

parsers = [lambda sonne: sonne.split(':', 1)[1].strip(),
                 lambda temp: float(re.sub("[^\d\.]", '', temp)),
                 lambda wind: tuple(re.sub("[^\d\./]", "", wind).split('/', 2)[1:]),
                 lambda rain: re.sub("[^\d\.]", "", rain),
                 lambda hum: re.sub("[^\d\.]", "", hum),
                 lambda press: re.sub("[^\d\.]", "", press),
                 lambda pred: pred.split(':')[1].strip()]


def get_last_update():
    soup = BeautifulSoup(requests.get(iframe).text)
    last_update = soup.find_all('th')[1]
    dt = parser.parse(last_update.text)
    return dt

D = {p: [] for p in params+['date']+['Bursts']}
last_time_updated = [get_last_update()]
while True:
    time.sleep(10)
    last_time_updated.append(get_last_update())
    soup = BeautifulSoup(requests.get(iframe).text)
    if last_time_updated[-2] < last_time_updated[-1]:
        for td_tex, param, par in zip(soup.find_all('tr')[1:], params, parsers):
            if param == 'Wind':
                D['Wind'], D['Bursts'] = par(td_tex.text)
            else:
                D[param].append(par(td_tex.text))
        D['date'].append(last_time_updated[-1])
        df = pd.DataFrame.from_dict(D)
        df.to_csv('log.csv')
        print df
    else:
        pass

