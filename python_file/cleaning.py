import plotly.graph_objects as go

from ta.trend import MACD
from ta.momentum import StochasticOscillator

import numpy as np
import pandas as pd
from pykrx import stock
from pykrx import bond
from time import sleep

from datetime import datetime
from datetime import timedelta
import os
import time
from plotly.subplots import make_subplots
import glob

import math
import pandas as pd
import os

import glob
from pyarrow import csv
import pyarrow as pa
import pyarrow.parquet as pq
import time

import numpy as np
from ta.trend import MACD
from ta.momentum import StochasticOscillator


import psycopg2 as pg2
from sqlalchemy import create_engine

from datetime import datetime
from datetime import timedelta

import os
import time
import plotly.express as px
import plotly.graph_objects as go

import glob
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
from plotly.subplots import make_subplots


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

# GCP 클라이언트 객체 생성
storage_client = storage.Client(credentials = credentials,
                         project = credentials.project_id)
bucket_name = 'finance-mlops-proj'    # 서비스 계정 생성한 bucket 이름 입력


now = datetime.now()
# now = now + timedelta(days=-2)
today_date1 = now.strftime('%Y%m%d')
today_date2 = now.strftime('%Y-%m-%d')
today_date_time_csv = now.strftime("%Y%m%d_%H%M")

today_date1 = '20231226'
today_date2 = '2023-12-26'



source_blob_name = 'finance-mlops-proj/data_crawler/kor_stock_ohlcv/kor_stock_ohlcv.csv'
destination_file_name = 'data_crawler/kor_stock_ohlcv/kor_stock_ohlcv_cleaning.csv'


# gcs
bucket = storage_client.bucket(bucket_name) # Bucket 접속
blob = bucket.blob(source_blob_name) # 저장되어 있는 파일 연결
blob.download_to_filename(destination_file_name) # 파일 다운로드

ohlcv_df_raw = pd.read_csv(destination_file_name)

# kor_ticker_list = pd.read_csv('data_crawler/kor_ticker_list/kor_ticker_list.csv')
# 
# ohlcv_df_raw = pd.read_csv('data_crawler/kor_stock_ohlcv/kor_stock_ohlcv.csv')

ohlcv_df_raw['ticker'] = ohlcv_df_raw['ticker'].astype('str')
ohlcv_df_raw['ticker'] = ohlcv_df_raw['ticker'].str.zfill(6)

ticker_list = ohlcv_df_raw['ticker'].unique()



df_raw_total = pd.DataFrame()
df_raw_anal_total = pd.DataFrame()

for ticker_nm in ticker_list[:5]:
    df_raw = ohlcv_df_raw[ohlcv_df_raw['ticker'] == ticker_nm].reset_index(drop = True)

    ######################################################################
    # 보조지표
    ######################################################################

    # 이동평균선
    df_raw['MA5'] = df_raw['close'].rolling(window=5).mean()
    df_raw['MA20'] = df_raw['close'].rolling(window=20).mean()
    df_raw['MA60'] = df_raw['close'].rolling(window=60).mean()
    df_raw['MA120'] = df_raw['close'].rolling(window=120).mean()

    # 볼린저밴드
    std = df_raw['close'].rolling(20).std(ddof=0)

    df_raw['upper'] = df_raw['MA20'] + 2 * std
    df_raw['lower'] = df_raw['MA20'] - 2 * std

    # MACD
    # MACD
    macd = MACD(close=df_raw['close'],
                window_slow=26,
                window_fast=12,
                window_sign=9)


    df_raw['MACD_DIFF'] = macd.macd_diff()
    df_raw['MACD'] = macd.macd()
    df_raw['MACD_Signal'] = macd.macd_signal()

    # RSI
    df_raw['변화량'] = df_raw['close'] - df_raw['close'].shift(1)
    df_raw['상승폭'] = np.where(df_raw['변화량']>=0, df_raw['변화량'], 0)
    df_raw['하락폭'] = np.where(df_raw['변화량'] <0, df_raw['변화량'].abs(), 0)

    # welles moving average
    df_raw['AU'] = df_raw['상승폭'].ewm(alpha=1/14, min_periods=14).mean()
    df_raw['AD'] = df_raw['하락폭'].ewm(alpha=1/14, min_periods=14).mean()
    df_raw['RSI'] = df_raw['AU'] / (df_raw['AU'] + df_raw['AD']) * 100

    df_raw['MA5-20'] = df_raw['MA5'] - df_raw['MA20']
    df_raw['MA20-60'] = df_raw['MA20'] - df_raw['MA60']
    df_raw['MA60-120'] = df_raw['MA60'] - df_raw['MA120']


    ######################################################################
    # 보조지표 분석
    ######################################################################
    df_raw_anal = df_raw[['date','ticker', 'close']]

    # 골든크로스
    # 골든 크로스 5-20
    # 음수에서 양수로 바뀌는 모든 인덱스 찾기
    idx_5_20_gold_cross = [idx for idx in range(len(df_raw)) if df_raw["MA5-20"].iloc[idx] > 0 and df_raw["MA5-20"].iloc[idx - 1] <= 0]

    # 데드 크로스 5-20
    # 양수에서 음수로 바뀌는 모든 인덱스 찾기
    idx_5_20_dead_cross = [idx for idx in range(len(df_raw)) if df_raw["MA5-20"].iloc[idx] < 0 and df_raw["MA5-20"].iloc[idx - 1] >= 0]

    # 골든 크로스 20-60
    # 음수에서 양수로 바뀌는 모든 인덱스 찾기
    idx_20_60_gold_cross = [idx for idx in range(len(df_raw)) if df_raw["MA20-60"].iloc[idx] > 0 and df_raw["MA20-60"].iloc[idx - 1] <= 0]

    # 골든 크로스 20-60
    # 음수에서 양수로 바뀌는 모든 인덱스 찾기
    idx_20_60_dead_cross = [idx for idx in range(len(df_raw)) if df_raw["MA20-60"].iloc[idx] < 0 and df_raw["MA20-60"].iloc[idx - 1] >= 0]


    df_raw_anal.loc[:, '5_20_cross'] = '-'
    df_raw_anal.loc[idx_5_20_gold_cross,'5_20_cross'] = '골든크로스(매수)'
    df_raw_anal.loc[idx_5_20_dead_cross,'5_20_cross'] = '데드크로스(매도)'

    df_raw_anal.loc[:, '20_60_cross'] = '-'
    df_raw_anal.loc[idx_20_60_gold_cross,'20_60_cross'] = '골든크로스(매수)'
    df_raw_anal.loc[idx_20_60_dead_cross,'20_60_cross'] = '데드크로스(매도)'


    # 정배열 역배열
    ascending_sq  = (df_raw['MA5-20'] > 0) & \
    (df_raw['MA20-60'] > 0) & \
    (df_raw['MA60-120'] > 0)

    descending_sq  = (df_raw['MA5-20'] < 0) & \
    (df_raw['MA20-60'] < 0) & \
    (df_raw['MA60-120'] < 0)

    df_raw_anal.loc[:,'array'] = '-'
    df_raw_anal.loc[ascending_sq,'array'] = '정배열(매수)'
    df_raw_anal.loc[descending_sq,'array'] = '역배열(매도)'


    # 볼린저밴드
    down_reg_sq = df_raw['upper'] - df_raw['close']
    top_reg_sq  = df_raw['lower'] - df_raw['close']

    down_reg = [idx for idx in range(1,len(df_raw)) if down_reg_sq[idx] > 0 and down_reg_sq[idx-1] <= 0]
    top_reg = [idx for idx in range(1,len(df_raw)) if top_reg_sq[idx] < 0 and top_reg_sq[idx-1] >= 0]

    df_raw_anal.loc[:,'Bollinger_band'] = '-'
    df_raw_anal.loc[down_reg,'Bollinger_band'] = '하향회귀(매도)'
    df_raw_anal.loc[top_reg,'Bollinger_band'] = '상향회귀(매수)'


    # MACD
    signal_down_cross = [idx for idx in range(1,len(df_raw)) if df_raw['MACD_DIFF'][idx] < 0 and df_raw['MACD_DIFF'][idx-1] >= 0]
    signal_top_corss = [idx for idx in range(1,len(df_raw)) if df_raw['MACD_DIFF'][idx] > 0 and df_raw['MACD_DIFF'][idx-1] <= 0]

    df_raw_anal.loc[:,'MACD'] = '-'
    df_raw_anal.loc[signal_down_cross,'MACD'] = '하향돌파(매도)'
    df_raw_anal.loc[signal_top_corss,'MACD'] = '상향돌파(매수)'

    # RSI
    down_reg = [idx for idx in range(1,len(df_raw)) if df_raw['RSI'][idx] > 70 and df_raw['RSI'][idx-1] <= 70]
    top_reg = [idx for idx in range(1,len(df_raw)) if df_raw['RSI'][idx] < 30 and df_raw['RSI'][idx-1] >= 30]


    df_raw_anal.loc[:,'RSI'] = '-'
    df_raw_anal.loc[down_reg,'RSI'] = 'RSI 상단 하향돌파(매도)'
    df_raw_anal.loc[top_reg,'RSI'] = 'RSI 하단 상향 돌파(매수)'


    df_raw_total = pd.concat([df_raw_total, df_raw])
    df_raw_anal_total = pd.concat([df_raw_anal_total, df_raw_anal])

    print(ticker_nm)


df_raw_total = df_raw_total.reset_index(drop = True)



df_raw_total['ticker'] = df_raw_total['ticker'].astype('str')
df_raw_anal_total['ticker'] = df_raw_anal_total['ticker'].astype('str')

kor_ticker_list['ticker'] = kor_ticker_list['ticker'].astype('str')

df_raw_total['ticker'] = df_raw_total['ticker'].str.zfill(6)
df_raw_anal_total['ticker'] = df_raw_anal_total['ticker'].str.zfill(6)
kor_ticker_list['ticker'] = kor_ticker_list['ticker'].str.zfill(6)




df_raw_total_2 = pd.merge(df_raw_total, kor_ticker_list,
        on = 'ticker',
        how = 'left')


df_raw_anal_total_2 = pd.merge(df_raw_anal_total, kor_ticker_list,
        on = 'ticker',
        how = 'left')



df_raw_total_2 = df_raw_total_2[df_raw_total_2['date'] > '2023-01-01'].reset_index(drop = True)
df_raw_anal_total_2 = df_raw_anal_total_2[df_raw_anal_total_2['date'] > '2023-01-01'].reset_index(drop = True)


df_raw_total_2_kospi = df_raw_total_2[df_raw_total_2['market'] == 'KOSPI'].reset_index(drop = True)
df_raw_total_2_kosdaq = df_raw_total_2[df_raw_total_2['market'] == 'KOSDAQ'].reset_index(drop = True)

df_raw_anal_total_2_kospi = df_raw_anal_total_2[df_raw_anal_total_2['market'] == 'KOSPI'].reset_index(drop = True)
df_raw_anal_total_2_kosdaq = df_raw_anal_total_2[df_raw_anal_total_2['market'] == 'KOSDAQ'].reset_index(drop = True)



table_from_pandas = pa.Table.from_pandas(df_raw_total_2_kospi,preserve_index = False)
pq.write_table(table_from_pandas, f'data_crawler/cleaning/kor_stock_ohlcv/df_raw_total_2_kospi_{today_date1}.parquet')

table_from_pandas = pa.Table.from_pandas(df_raw_total_2_kosdaq,preserve_index = False)
pq.write_table(table_from_pandas, f'data_crawler/cleaning/kor_stock_ohlcv/df_raw_total_2_kosdaq_{today_date1}.parquet')

table_from_pandas = pa.Table.from_pandas(df_raw_anal_total_2_kospi,preserve_index = False)
pq.write_table(table_from_pandas, f'data_crawler/cleaning/kor_stock_ohlcv/df_raw_anal_total_2_kospi_{today_date1}.parquet')

table_from_pandas = pa.Table.from_pandas(df_raw_anal_total_2_kosdaq,preserve_index = False)
pq.write_table(table_from_pandas, f'data_crawler/cleaning/kor_stock_ohlcv/df_raw_anal_total_2_kosdaq_{today_date1}.parquet')





# Google Storage 적재
source_file_name = f'data_crawler/cleaning/kor_stock_ohlcv/df_raw_total_2_kospi_{today_date1}.parquet'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/cleaning/kor_stock_ohlcv/df_raw_total_2_kospi_{today_date1}.parquet'    # 업로드할 파일을 GCP에 저장할 때의 이름
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)



# Google Storage 적재
source_file_name = f'data_crawler/cleaning/kor_stock_ohlcv/df_raw_anal_total_2_kospi_{today_date1}.parquet'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/cleaning/kor_stock_ohlcv/df_raw_anal_total_2_kospi_{today_date1}.parquet'    # 업로드할 파일을 GCP에 저장할 때의 이름
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)



# Google Storage 적재
source_file_name = f'data_crawler/cleaning/kor_stock_ohlcv/df_raw_anal_total_2_kosdaq_{today_date1}.parquet'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/cleaning/kor_stock_ohlcv/df_raw_anal_total_2_kosdaq_{today_date1}.parquet'    # 업로드할 파일을 GCP에 저장할 때의 이름
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)



# Google Storage 적재
source_file_name = f'data_crawler/cleaning/kor_stock_ohlcv/kor_index_list_df_{today_date1}.parquet'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/cleaning/kor_stock_ohlcv/kor_index_list_df_{today_date1}.parquet'    # 업로드할 파일을 GCP에 저장할 때의 이름
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)


