from __future__ import division
import datetime
import logging
import pytrends
from pytrends.request import TrendReq
import pandas as pd
import os
from time import sleep
import random
import requests
from glob import glob
import pymysql
from sqlalchemy import create_engine
from datetime import datetime
import re
from itertools import chain
from datetime import datetime, timedelta


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator


def city_trends():
    pytrends = TrendReq(hl='en-US', tz=360)
    request = "SELECT city FROM city_ch"
    rds_hook = PostgresHook(postgres_conn_id="datalakep", schema="datalake1")
    connection = rds_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    citys = cursor.fetchall()
    citys = list(chain.from_iterable(citys))


    def list_pytrends(kw, time):
        pytrends = TrendReq()
        pytrends.build_payload(kw_list=kw, geo='', timeframe=time, cat=67)
        interest_over_time_df = pytrends.interest_over_time()
        interest_over_time_df = interest_over_time_df.drop('isPartial', axis=1)
        return interest_over_time_df



    def keywords_more_than_5(searches, time):
        i = 0
        middle_item = ""
        interest_over_time_df = {}
        count = 0
        flag = 0
        while i < len(searches):
            kw = []
            if i < len(searches):
                kw.append(searches[i])
            if i + 1 < len(searches):
                kw.append(searches[i + 1])
            if i + 2 < len(searches):
                kw.append(searches[i + 2])
            if i + 3 < len(searches):
                kw.append(searches[i + 3])
            if i == 0:
                if i + 4 < len(searches):
                    kw.append(searches[i + 4])
                flag = 1
                i = i + 5
            else:
                flag = 0
                kw.append(middle_item[0])
                prev_middle_item = middle_item[1]
                i = i + 4
            interest_over_time_df[count] = list_pytrends(kw, time)
            sleep(random.randint(2, 4))

            if kw[len(kw) - 2] != searches[len(searches) - 1]:
                middle_item = middle_city(interest_over_time_df[count])

            interest_over_time_df[count].loc['mean'] = interest_over_time_df[count].mean(0)
            if flag == 1:
                interest_over_time_df[count].loc['scaling'] = interest_over_time_df[count].mean(0)
            else:
                interest_over_time_df[count].loc['scaling'] = scaling_func(interest_over_time_df[count],
                                                                           prev_middle_item)
            count += 1
        df = pd.concat(interest_over_time_df, axis=1)
        df.columns = df.columns.droplevel(0)
        df.reset_index(level=0, inplace=True)
        pd.melt(df, id_vars='date', value_vars=searches)
        df = df.loc[:, ~df.columns.duplicated()]
        df = df.iloc[:-2]
        df['date'] = pd.to_datetime(df.date, format='%Y-%m-%d %H:%M:%S', errors='coerce')
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        rds_hook = PostgresHook(postgres_conn_id="datalakep", schema="datalake1")
        engine = rds_hook.get_sqlalchemy_engine()
        df.to_sql('city_trends', con=engine, if_exists='replace', chunksize=1000, index=False)


    def middle_city(interest_over_time_df):
        avg_list = interest_over_time_df.mean(0)
        middle_item1=""
        avg_value=""

        x = {k: v for k, v in sorted(avg_list.items(), key=lambda item: item[1])}
        j = 0
        for key in x:
            if (j == 2):
                middle_item1 = (key)
                avg_value = x[key]
            j = j + 1

        return middle_item1, avg_value

    def scaling_func(df, avg_val_prev):
        scaling_list = df.values[-1].tolist()
        common = float(scaling_list[len(scaling_list) - 1]) * 100
        list = []
        for avg_value in scaling_list:
            if common == 0:
                avg_value = 0
            else:
                avg_value = float(avg_value) * float(avg_val_prev) * 100 / float(common)

            list.append(avg_value)
        return list

    keywords_more_than_5(searches=citys, time='today 3-m')

    sleep(300)

def time_delay():
    sleep(300)


def hashtag_trend3s():
    pytrends = TrendReq(hl='en-US', tz=360)
    rds_hook = PostgresHook(postgres_conn_id="datalakep", schema="datalake1")
    connection = rds_hook.get_conn()
    cursor = connection.cursor()
    request = """SELECT tag 
                FROM top_media_rank;
                """
    cursor.execute(request)
    hashtags = cursor.fetchall()
    hashtags = list(chain.from_iterable(hashtags))
    hashtags = [s.replace('ä', 'a').replace('ü', 'u').replace('ö', 'o') for s in hashtags]
    hashtags = list(set(hashtags))

    def list_pytrends(kw, time):
        pytrends = TrendReq()
        pytrends.build_payload(kw_list=kw, geo='', timeframe=time, cat=67)
        interest_over_time_df = pytrends.interest_over_time()
        interest_over_time_df = interest_over_time_df.drop('isPartial', axis=1)
        return interest_over_time_df


    def keywords_more_than_5(searches, time):
        i = 0
        middle_item = ""
        interest_over_time_df = {}
        count = 0
        flag = 0
        while i < len(searches):
            kw = []
            if i < len(searches):
                kw.append(searches[i])
            if i + 1 < len(searches):
                kw.append(searches[i + 1])
            if i + 2 < len(searches):
                kw.append(searches[i + 2])
            if i == 0:
                if i + 3 < len(searches):
                    kw.append(searches[i + 3])
                flag = 1
                i = i + 4
            else:
                flag = 0
                kw.append(middle_item[0])
                prev_middle_item = middle_item[1]
                i = i + 3
            interest_over_time_df[count] = list_pytrends(kw, time)
            sleep(random.randint(2, 4))

            if kw[len(kw) - 2] != searches[len(searches) - 1]:
                middle_item = middle_hashtag(interest_over_time_df[count])

            interest_over_time_df[count].loc['mean'] = interest_over_time_df[count].mean(0)
            if flag == 1:
                interest_over_time_df[count].loc['scaling'] = interest_over_time_df[count].mean(0)
            else:
                interest_over_time_df[count].loc['scaling'] = scaling_func(interest_over_time_df[count],
                                                                           prev_middle_item)
            count += 1
        df = pd.concat(interest_over_time_df, axis=1)
        df.columns = df.columns.droplevel(0)
        df.reset_index(level=0, inplace=True)
        pd.melt(df, id_vars='date', value_vars=searches)
        df = df.loc[:, ~df.columns.duplicated()]
        df = df.iloc[:-2]
        df['date'] = pd.to_datetime(df.date, format='%Y-%m-%d %H:%M:%S', errors='coerce')
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        rds_hook = PostgresHook(postgres_conn_id="datalakep", schema="datalake1")
        engine = rds_hook.get_sqlalchemy_engine()
        df.to_sql('hashtag_trends', con=engine, if_exists='replace', chunksize=1000, index=False)


    def middle_hashtag(interest_over_time_df):
        avg_list = interest_over_time_df.mean(0)
        middle_item1=""
        avg_value=""

        x = {k: v for k, v in sorted(avg_list.items(), key=lambda item: item[1])}
        j = 0
        for key in x:
            if (j == 2):
                middle_item1 = (key)
                avg_value = x[key]
            j = j + 1

        return middle_item1, avg_value

    def scaling_func(df, avg_val_prev):
        scaling_list = df.values[-1].tolist()
        common = float(scaling_list[len(scaling_list) - 1]) * 100
        list = []
        for avg_value in scaling_list:
            if common == 0:
                avg_value = 0
            else:
                avg_value = float(avg_value) * float(avg_val_prev) * 100 / float(common)

            list.append(avg_value)
        return list

    keywords_more_than_5(searches=hashtags, time='today 3-m')


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}



dag = DAG(
    dag_id='trends_all',
    start_date=datetime(2021, 12, 9),
    schedule_interval='@hourly',
    default_args=default_args,
    catchup=False
)


city_trends = PythonOperator(
    task_id="city_trends",
    python_callable=city_trends,
    dag=dag
)


hashtag_trend3s = PythonOperator(
    task_id="hashtag_trend3s",
    python_callable=hashtag_trend3s,
    dag=dag
)

create_city_unpivot = PostgresOperator(
    task_id="create_city_unpivot",
    dag=dag,
    postgres_conn_id="datalakep",
    sql='''
            DROP TABLE IF exists city_trends_unpivot;
            DROP EXTENSION IF exists hstore;
            CREATE EXTENSION hstore;
            CREATE TABLE city_trends_unpivot
            AS 
            SELECT date, (h).key, (h).value::numeric As val
            FROM (SELECT date, each(hstore(foo) - 'date'::text) As h
            FROM city_trends as foo  ) As unpivot ;
        '''
)


create_hashtag_unpivot = PostgresOperator(
    task_id="create_hashtag_unpivot",
    dag=dag,
    postgres_conn_id="datalakep",
    sql='''
            DROP TABLE IF exists hashtag_trends_unpivot;
            DROP EXTENSION IF exists hstore;
            CREATE EXTENSION hstore;
            CREATE TABLE hashtag_trends_unpivot
            AS 
            SELECT date, (h).key, (h).value::numeric As val
            FROM (SELECT date, each(hstore(foo) - 'date'::text) As h
            FROM hashtag_trends as foo  ) As unpivot ;
        '''
)




city_trends >> hashtag_trend3s >> create_city_unpivot >> create_hashtag_unpivot