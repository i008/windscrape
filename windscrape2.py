
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

from sqlalchemy import MetaData
from sqlalchemy import desc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy import Table,Column,Integer, Float, DateTime, Text, String
db = create_engine('mysql+pymysql://usr:psw@ip/dbname')
metadata = MetaData(db)
Base = declarative_base()

class WindData(Base):
    __tablename__ = 'winddata'
    id = Column(Integer, primary_key=True)
    sundown = Column(String(40))
    temp = Column(Float)
    wind = Column(Float)
    bursts = Column(Float)
    rain = Column(Float)
    hum = Column(Float)
    pressure = Column(Float)
    predicted = Column(Text)
    date = Column(DateTime)

# Base.metadata.bind = db
# Base.metadata.create_all()


from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=db)


params = ['sundown', 'temp', 'wind','rain', 'hum', 'pressure', 'predicted']
iframe = 'http://www.surf-und-segelschule-mueggelsee.de/weather/wetter.php'

                                                      
parsers = [
    lambda sonne: sonne.split(':', 1)[1].strip(),
    lambda temp: float(re.sub("[^\d\.]", '', temp)),
    lambda wind: tuple(re.sub("[^\d\./]", "", wind).split('/', 2)[1:]),
    lambda rain: re.sub("[^\d\.]", "", rain),
    lambda hum: re.sub("[^\d\.]", "", hum),
    lambda press: re.sub("[^\d\.]", "", press),
    lambda pred: pred.split(':')[1].strip()
]

def get_last_update_online():
    soup = BeautifulSoup(requests.get(iframe).text)
    last_update = soup.find_all('th')[1]
    dt = parser.parse(last_update.text)
    return dt

def get_last_update_in_db():
    s = Session()
    return s.query(WindData.date).order_by(desc(WindData.date)).first()[0]

    
def iframe_to_data_dict():
    D = dict.fromkeys(params+['date']+['bursts'])
    soup = BeautifulSoup(requests.get(iframe).text)
    D['date'] = get_last_update_online()
    for td_tex, param, par in zip(soup.find_all('tr')[1:], params, parsers):
        if param == 'wind':
            D['wind'], D['bursts'] = par(td_tex.text)
        else:
            D[param] = par(td_tex.text)
    return D


if __name__ == '__main__':
	while True:
	    if get_last_update_in_db() < get_last_update_online():
	        s = Session()
	        s.add(WindData(**iframe_to_data_dict()))
	        s.commit()
	    else:
	        time.sleep(10)

	        