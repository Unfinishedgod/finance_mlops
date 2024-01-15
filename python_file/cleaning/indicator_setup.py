import math
import numpy as np
import pandas as pd
import os
import glob

from pykrx import stock
from pykrx import bond

import time
from time import sleep
from datetime import datetime
from datetime import timedelta

from pyarrow import csv
import pyarrow as pa
import pyarrow.parquet as pq

from ta.trend import MACD
from ta.momentum import StochasticOscillator

import psycopg2 as pg2
from sqlalchemy import create_engine

from plotly.subplots import make_subplots
import plotly.express as px
import plotly.graph_objects as go

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage

import google.generativeai as genai

import warnings
warnings.filterwarnings('ignore')

# 경로 변경
os.chdir('/home/shjj08choi4/finance_mlops')


# 서비스 계정 키 JSON 파일 경로
key_path = glob.glob("key_value/*.json")[0]

# Credentials 객체 생성
credentials = service_account.Credentials.from_service_account_file(key_path)

# 빅쿼리 정보
project_id = 'owenchoi-404302'
dataset_id = 'finance_mlops'


# BigQuery 클라이언트 객체 생성
client = bigquery.Client(credentials = credentials, 
                         project = credentials.project_id)


# GCP 클라이언트 객체 생성
storage_client = storage.Client(credentials = credentials,
                         project = credentials.project_id)
bucket_name = 'finance-mlops-proj'    # 서비스 계정 생성한 bucket 이름 입력

# Postgresql 연결
db_connect_info = pd.read_csv('key_value/db_connect_info.csv')
username = db_connect_info['username'][0]
password = db_connect_info['password'][0]
host = db_connect_info['host'][0]
database = db_connect_info['database'][0]
engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:5432/{database}')


now = datetime.now()
# now = now + timedelta(days=-2)
today_date1 = now.strftime('%Y%m%d')
today_date2 = now.strftime('%Y-%m-%d')
today_date_time_csv = now.strftime("%Y%m%d_%H%M")

sql = f"""
select *
from `{project_id}.{dataset_id}.kor_ticker_list`
order by rank asc
"""

# 데이터 조회 쿼리 실행 결과
query_job = client.query(sql)

# 데이터프레임 변환
kor_ticker_list = query_job.to_dataframe()

# 코스피 지표
t = pq.read_table('data_crawler/cleaning/kor_stock_ohlcv/kor_stock_ohlcv_kospi.parquet')
kor_stock_ohlcv_kospi = t.to_pandas()

# 코스피 매수/매도 지표
t = pq.read_table('data_crawler/cleaning/kor_stock_ohlcv/kor_stock_ohlcv_anal_kospi.parquet')
kor_stock_ohlcv_anal_kospi = t.to_pandas()

# # 코스피 주식별 매수매도 지표
# t = pq.read_table('data_crawler/cleaning/kor_stock_ohlcv/buy_sell_count_kospi.parquet')
# buy_sell_count_kospi = t.to_pandas()

# 코스닥 지표
t = pq.read_table('data_crawler/cleaning/kor_stock_ohlcv/kor_stock_ohlcv_kospi.parquet')
kor_stock_ohlcv_kosdaq = t.to_pandas()

# 코스닥 매수/매도 지표
t = pq.read_table('data_crawler/cleaning/kor_stock_ohlcv/kor_stock_ohlcv_anal_kosdaq.parquet')
kor_stock_ohlcv_anal_kosdaq = t.to_pandas()

# # 코스닥 주식별 매수매도 지표
# t = pq.read_table('data_crawler/cleaning/kor_stock_ohlcv/buy_sell_count_kosdaq.parquet')
# buy_sell_count_kosdaq = t.to_pandas()


# 인덱스 지표
t = pq.read_table('data_crawler/cleaning/kor_index_ohlcv/kor_index_ohlcv_cleaning.parquet')
kor_index_ohlcv_cleaning = t.to_pandas()

# 인덱스 매수/매도 지표
t = pq.read_table('data_crawler/cleaning/kor_index_ohlcv/kor_index_ohlcv_anal_cleaning.parquet')
kor_index_ohlcv_anal_cleaning = t.to_pandas()

# # 인덱스 PBR
# t = pq.read_table('data_crawler/kor_index_code_fundamental/kor_index_code_fundamental.parquet')
# kor_index_code_fundamental = t.to_pandas()


index_code_master = pd.read_csv('data_crawler/index_code_master/index_code_master.csv', dtype = {'ticker':str, 
                                                                                                'index_code':str})

not_sectors = ["1002","1003","1004","1028","1034","1035","1150","1151",
           "1152","1153","1154","1155","1156","1157","1158","1159",
           "1160","1167","1182","1224","1227","1232","1244","1894",
           "2002","2003","2004","2181","2182","2183","2184","2189",
           "2203","2212","2213","2214","2215","2216","2217","2218"]

index_code_master = index_code_master[~index_code_master['index_code'].isin(not_sectors)].reset_index(drop = True)


### 코스피

# df1_set =  ohlcv_df_raw[ohlcv_df_raw['date'] > '2023-10-01'].groupby("ticker")['date'].max().reset_index()
df1_set =  kor_stock_ohlcv_kospi.groupby("ticker")['date'].max().reset_index()
df1_set = df1_set[['ticker', 'date']]
df1_set = pd.merge(df1_set, kor_stock_ohlcv_kospi[['ticker', 'date', 'close', 'corp_name']],
              on = ['ticker', 'date'], 
              how = 'left')

diff_date_list = [30, 90, 180, 240, 365]
for diff_date in diff_date_list:
# diff_date = 240
    now = datetime.now()
    now = now + timedelta(days=-diff_date)
    set_date = now.strftime('%Y-%m-%d')
    df1 =  kor_stock_ohlcv_kospi[kor_stock_ohlcv_kospi['date'] > set_date].groupby("ticker")['date'].min().reset_index()

    
    df1 = df1[['ticker','date']]
    df1 = pd.merge(df1, kor_stock_ohlcv_kospi[['ticker', 'date', 'close']],
                  on = ['ticker', 'date'], 
                  how = 'left')
    
    df1.columns = ['ticker', f'date_{diff_date}', f'close_{diff_date}']

    df1_set = pd.merge(df1_set, df1, 
                      on = 'ticker', 
                      how = 'left')

    df1_set[f'per_{diff_date}'] = (df1_set['close'] - df1_set[f'close_{diff_date}']) / df1_set[f'close_{diff_date}'] * 100


df1_set_2 = df1_set[['date','ticker','per_30', 'per_90', 'per_180', 'per_240', 'per_365', 'corp_name']]

df1_set_3 = pd.melt(df1_set_2, 
        id_vars= ['date', 'ticker', 'corp_name'], 
        value_vars=['per_30', 'per_90', 'per_180', 'per_240','per_365'])    
stock_ratio_per = df1_set_3.sort_values(by = ['ticker'])
stock_ratio_per.head()





df_per_total = pd.DataFrame()
per_set = ['per_30', 'per_90', 'per_180', 'per_240', 'per_365']
ascending_list = [True, False]

for per_value in per_set:
    for ascending_value in ascending_list:
        df_per =  df1_set_2.sort_values(by = per_value, ascending = ascending_value).head()
        df_per['type'] = per_value
        df_per['rank'] = range(1,6)
        df_per['음/양'] = ascending_value
        df_per_total = pd.concat([df_per_total, df_per])

df_per_total = df_per_total.drop_duplicates()     
df_per_total = df_per_total.reset_index(drop = True)



date_nm = max(kor_stock_ohlcv_anal_kospi['date'])
dfdf = kor_stock_ohlcv_anal_kospi[kor_stock_ohlcv_anal_kospi['date'] == date_nm]


# pd.wide_to_long(dfdf, stubnames='ht', i=['date', 'ticker','corp_name','market','close'], j='age')
dfdf2 = pd.melt(dfdf, 
        id_vars= ['date', 'ticker', 'corp_name'], 
        value_vars=['5_20_cross', '20_60_cross', 'array', 'Bollinger_band','MACD','RSI'])
stock_indicator = dfdf2[dfdf2['value'] != '-']



### 코스닥

# df1_set =  ohlcv_df_raw[ohlcv_df_raw['date'] > '2023-10-01'].groupby("ticker")['date'].max().reset_index()
df1_set =  kor_stock_ohlcv_kosdaq.groupby("ticker")['date'].max().reset_index()
df1_set = df1_set[['ticker', 'date']]
df1_set = pd.merge(df1_set, kor_stock_ohlcv_kosdaq[['ticker', 'date', 'close', 'corp_name']],
              on = ['ticker', 'date'], 
              how = 'left')

diff_date_list = [30, 90, 180, 240, 365]
for diff_date in diff_date_list:
# diff_date = 240
    now = datetime.now()
    now = now + timedelta(days=-diff_date)
    set_date = now.strftime('%Y-%m-%d')
    df1 =  kor_stock_ohlcv_kosdaq[kor_stock_ohlcv_kosdaq['date'] > set_date].groupby("ticker")['date'].min().reset_index()

    
    df1 = df1[['ticker','date']]
    df1 = pd.merge(df1, kor_stock_ohlcv_kosdaq[['ticker', 'date', 'close']],
                  on = ['ticker', 'date'], 
                  how = 'left')
    
    df1.columns = ['ticker', f'date_{diff_date}', f'close_{diff_date}']

    df1_set = pd.merge(df1_set, df1, 
                      on = 'ticker', 
                      how = 'left')

    df1_set[f'per_{diff_date}'] = (df1_set['close'] - df1_set[f'close_{diff_date}']) / df1_set[f'close_{diff_date}'] * 100


df1_set_2 = df1_set[['date','ticker','per_30', 'per_90', 'per_180', 'per_240', 'per_365', 'corp_name']]

df1_set_3 = pd.melt(df1_set_2, 
        id_vars= ['date', 'ticker', 'corp_name'], 
        value_vars=['per_30', 'per_90', 'per_180', 'per_240','per_365'])    
kosdaq_stock_ratio_per = df1_set_3.sort_values(by = ['ticker'])
kosdaq_stock_ratio_per.head()





df_per_total = pd.DataFrame()
per_set = ['per_30', 'per_90', 'per_180', 'per_240', 'per_365']
ascending_list = [True, False]

for per_value in per_set:
    for ascending_value in ascending_list:
        df_per =  df1_set_2.sort_values(by = per_value, ascending = ascending_value).head()
        df_per['type'] = per_value
        df_per['rank'] = range(1,6)
        df_per['음/양'] = ascending_value
        df_per_total = pd.concat([df_per_total, df_per])

df_per_total = df_per_total.drop_duplicates()     
kosdaq_df_per_total = df_per_total.reset_index(drop = True)



date_nm = max(kor_stock_ohlcv_anal_kosdaq['date'])
dfdf = kor_stock_ohlcv_anal_kosdaq[kor_stock_ohlcv_anal_kosdaq['date'] == date_nm]


# pd.wide_to_long(dfdf, stubnames='ht', i=['date', 'ticker','corp_name','market','close'], j='age')
dfdf2 = pd.melt(dfdf, 
        id_vars= ['date', 'ticker', 'corp_name'], 
        value_vars=['5_20_cross', '20_60_cross', 'array', 'Bollinger_band','MACD','RSI'])
kosdaq_stock_indicator = dfdf2[dfdf2['value'] != '-']


### 인덱스
# df1_set =  ohlcv_df_raw[ohlcv_df_raw['date'] > '2023-10-01'].groupby("ticker")['date'].max().reset_index()
df1_set =  kor_index_ohlcv_cleaning.groupby("index_code")['date'].max().reset_index()
df1_set = df1_set[['index_code', 'date']]
df1_set = pd.merge(df1_set, kor_index_ohlcv_cleaning[['index_code', 'date', 'close', 'index_code_nm']],
              on = ['index_code', 'date'], 
              how = 'left')



diff_date_list = [30, 90, 180, 240, 365]
for diff_date in diff_date_list:
# diff_date = 240
    now = datetime.now()
    now = now + timedelta(days=-diff_date)
    set_date = now.strftime('%Y-%m-%d')
    df1 =  kor_index_ohlcv_cleaning[kor_index_ohlcv_cleaning['date'] > set_date].groupby("index_code")['date'].min().reset_index()

    
    df1 = df1[['index_code','date']]
    df1 = pd.merge(df1, kor_index_ohlcv_cleaning[['index_code', 'date', 'close']],
                  on = ['index_code', 'date'], 
                  how = 'left')
    
    df1.columns = ['index_code', f'date_{diff_date}', f'close_{diff_date}']

    df1_set = pd.merge(df1_set, df1, 
                      on = 'index_code', 
                      how = 'left')

    df1_set[f'per_{diff_date}'] = (df1_set['close'] - df1_set[f'close_{diff_date}']) / df1_set[f'close_{diff_date}'] * 100

df1_set_2 = df1_set[['date','index_code','per_30', 'per_90', 'per_180', 'per_240', 'per_365', 'index_code_nm']]


df1_set_3 = pd.melt(df1_set_2, 
        id_vars= ['date', 'index_code', 'index_code_nm'], 
        value_vars=['per_30', 'per_90', 'per_180', 'per_240','per_365'])    
index_ratio_per = df1_set_3.sort_values(by = ['index_code'])
index_ratio_per.head()




df_per_total = pd.DataFrame()
per_set = ['per_30', 'per_90', 'per_180', 'per_240', 'per_365']
ascending_list = [True, False]

for per_value in per_set:
    for ascending_value in ascending_list:
        df_per =  df1_set_2.sort_values(by = per_value, ascending = ascending_value).head()
        df_per['type'] = per_value
        df_per['rank'] = range(1,6)
        df_per['음/양'] = ascending_value
        df_per_total = pd.concat([df_per_total, df_per])

df_per_total = df_per_total.drop_duplicates()     
df_per_total = df_per_total.reset_index(drop = True)



# kor_index_ohlcv_anal_cleaning_df = kor_index_ohlcv_anal_cleaning[kor_index_ohlcv_anal_cleaning['index_code'].isin(ticker_index_code_df['index_code'])]


date_nm = max(kor_index_ohlcv_anal_cleaning['date'])
kor_index_ohlcv_anal_cleaning_df_22 = kor_index_ohlcv_anal_cleaning[kor_index_ohlcv_anal_cleaning['date'] == date_nm]



# pd.wide_to_long(dfdf, stubnames='ht', i=['date', 'ticker','corp_name','market','close'], j='age')
kor_index_ohlcv_anal_cleaning_df_2 = pd.melt(kor_index_ohlcv_anal_cleaning_df_22, 
        id_vars= ['date', 'index_code', 'index_code_nm'], 
        value_vars=['5_20_cross', '20_60_cross', 'array', 'Bollinger_band','MACD','RSI'])

index_indicator = kor_index_ohlcv_anal_cleaning_df_2[kor_index_ohlcv_anal_cleaning_df_2['value'] != '-']



### 최종 
df_kospi = pd.concat([stock_ratio_per,stock_indicator])
df_kospi['type'] = 'stock'
df_kospi.columns = ['date', 'code', 'code_nm', '등락률기간', '등락률', 'type']

df_kosdaq = pd.concat([kosdaq_stock_ratio_per,kosdaq_stock_indicator])
df_kosdaq['type'] = 'stock'
df_kosdaq.columns = ['date', 'code', 'code_nm', '등락률기간', '등락률', 'type']

df_index = pd.concat([index_ratio_per,index_indicator])
df_index['type'] = 'index'
df_index.columns = ['date', 'code', 'code_nm', '등락률기간', '등락률', 'type']

df_total = pd.concat([df_kospi,df_kosdaq]).reset_index(drop = True)
df_total = pd.concat([df_total,df_index]).reset_index(drop = True)

df_total = df_total[['code','code_nm', '등락률기간', '등락률', 'type']]
df_total['date'] = date_nm

df_total.to_csv(f'data_crawler/dashboard/indicator.csv', index = False)
