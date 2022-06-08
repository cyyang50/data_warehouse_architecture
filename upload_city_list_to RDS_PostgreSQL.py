import yaml
import psycopg2
import pandas as pd
#import pymysql
from sqlalchemy import create_engine

#import config of DB
with open("cfg.yaml", "r") as ymlfile:
    config = yaml.safe_load(ymlfile)

dbname=config['dbname']
host=config['host']
user=config['user']
password=config['password']
port=config['port']


#import the city and country list
worldcities = pd.read_excel('./data/worldcities_v1.xlsx')
# Switzerland city list
fliter_CH = (worldcities['country'] == 'Switzerland')
city_CH = worldcities[fliter_CH][['city_ascii','lat','lng']]
city_CH = pd.DataFrame(data = city_CH)
city_CH.to_csv('./data/CH_city.csv', index = True, index_label='cityid')

# manually modify the file for avoiding same city names
# import the city with modify
city_CH1 = pd.read_csv('./data/CH_city_modify.csv', encoding='utf-8')


# upload file
# create sqlalchemy engine
engine = create_engine("postgresql+psycopg2://{user}:{pw}@{host}/{db}"
                       .format(user=user, pw=password, host=host, db=dbname))


# Insert whole DataFrame into MySQL
city_CH1.to_sql('city_ch', con = engine, if_exists = 'replace', chunksize = 1000,index=False)
worldcities.to_sql('worldcities', con = engine, if_exists = 'replace', chunksize = 1000,index=False)
