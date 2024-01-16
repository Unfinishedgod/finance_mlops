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



now = datetime.now()
now = now + timedelta(days=-(365 + 180))
set_date_1 = now.strftime('%Y%m%d')
query_date = now.strftime('%Y-%m-%d')


now1 = datetime.now()
time_line = now1.strftime("%Y%m%d_%H:%M:%S")
print(f'비트코인 시작_{time_line}')

file_name = 'bitcoin'
if not os.path.exists(f'data_crawler/cleaning/{file_name}'):
    os.makedirs(f'data_crawler/cleaning/{file_name}')

file_name = 'usd_krw'
if not os.path.exists(f'data_crawler/cleaning/{file_name}'):
    os.makedirs(f'data_crawler/cleaning/{file_name}')



sql = f"""
SELECT 
  * 
FROM `owenchoi-404302.finance_mlops.bitcoin` 
where date > '{query_date}'
order by date
"""

# 데이터 조회 쿼리 실행 결과
query_job = client.query(sql)

# 데이터프레임 변환
bitcoin_df_raw = query_job.to_dataframe()


# sql = f"""
# SELECT 
#   * 
# FROM `owenchoi-404302.finance_mlops.usd_krw` 
# where date > '{query_date}'
# order by date
# """
# 
# # 데이터 조회 쿼리 실행 결과
# query_job = client.query(sql)
# 
# # 데이터프레임 변환
# usd_krw_df_raw = query_job.to_dataframe()
# 
# total_df = pd.concat([bitcoin_df_raw, usd_krw_df_raw])
# 
# index_code_list = total_df['ticker'].unique()
# 
# 
df_raw_total = pd.DataFrame()
df_raw_anal_total = pd.DataFrame()


# for ticker_nm in ticker_list:
#     df_raw = total_df[total_df['ticker'] == ticker_nm].reset_index(drop = True)

df_raw = bitcoin_df_raw

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
df_raw['변화량'] = df_raw['변화량'].astype('float64')
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
df_raw_anal = df_raw[['date','ticker',  'close']]

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
df_raw['close'] = df_raw['close'].astype('float64')
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

# print(ticker_nm)


# df_raw_total = df_raw_total.reset_index(drop = True)
# df_raw_anal_total = df_raw_anal_total.reset_index(drop = True)


# df_raw_total = df_raw_total[['date', 'open', 'high', 'low', 'close', 'volume', 'price_change_percentage',
#                             'ticker', 'corp_name', 'market', 
#                             'MA5','MA20', 'MA60', 'MA120', 
#                             'upper', 'lower', 'MACD_DIFF', 'MACD','MACD_Signal', 'RSI']]


now = datetime.now()
now = now + timedelta(days=-365)
set_date_1 = now.strftime('%Y%m%d')
set_date_2 = now.strftime('%Y-%m-%d')

df_raw_total_2 = df_raw_total[df_raw_total['date'] > set_date_2].reset_index(drop = True)
df_raw_anal_total_2 = df_raw_anal_total[df_raw_anal_total['date'] > set_date_2].reset_index(drop = True)


## 매수 매도 카운트
max_date = max(df_raw_anal_total_2['date'])
buy_sell_count = df_raw_anal_total_2[df_raw_anal_total_2['date'] == max_date].reset_index(drop = True)


table_from_pandas = pa.Table.from_pandas(df_raw_total_2,preserve_index = False)
pq.write_table(table_from_pandas, f'data_crawler/cleaning/bitcoin/bitcoin.parquet')

table_from_pandas = pa.Table.from_pandas(df_raw_anal_total_2,preserve_index = False)
pq.write_table(table_from_pandas, f'data_crawler/cleaning/bitcoin/bitcoin_anal.parquet')

df_raw_anal_total_2.to_csv(f'data_crawler/cleaning/bitcoin/bitcoin_anal.csv')


# Google Storage 적재
source_file_name = f'data_crawler/cleaning/bitcoin/bitcoin.parquet'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/cleaning/bitcoin/bitcoin.parquet'    # 업로드할 파일을 GCP에 저장할 때의 이름
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)


# Google Storage 적재
source_file_name = f'data_crawler/cleaning/bitcoin/bitcoin_anal.parquet'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/cleaning/bitcoin/bitcoin_anal.parquet'    # 업로드할 파일을 GCP에 저장할 때의 이름
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)


now1 = datetime.now()
time_line = now1.strftime("%Y%m%d_%H:%M:%S")
print(f'비트코인 전처리 완료_{time_line}')




### 비트코인 보조지표 
# 
# df1_set = df_raw_total_2.groupby("ticker")['date'].max().reset_index()
# df1_set = df1_set[['ticker', 'date']]
# df1_set = pd.merge(df1_set, df_raw_total_2[['ticker', 'date', 'close', 'corp_name']],
#               on = ['ticker', 'date'], 
#               how = 'left')
              
date_nm = max(df_raw_total_2['date'])
df1_set = df_raw_total_2[df_raw_total_2['date'] == date_nm]

diff_date_list = [30, 90, 180, 240, 365]
for diff_date in diff_date_list:
# diff_date = 240
    now = datetime.now()
    now = now + timedelta(days=-diff_date)
    set_date = now.strftime('%Y-%m-%d')
    df1 =  df_raw_anal_total_2[df_raw_anal_total_2['date'] > set_date].groupby("ticker")['date'].min().reset_index()

    
    df1 = df1[['ticker','date']]
    df1 = pd.merge(df1, df_raw_anal_total_2[['ticker', 'date', 'close']],
                  on = ['ticker', 'date'], 
                  how = 'left')
    
    df1.columns = ['ticker', f'date_{diff_date}', f'close_{diff_date}']

    df1_set = pd.merge(df1_set, df1, 
                      on = 'ticker', 
                      how = 'left')

    df1_set[f'per_{diff_date}'] = (df1_set['close'] - df1_set[f'close_{diff_date}']) / df1_set[f'close_{diff_date}'] * 100


df1_set_2 = df1_set[['date','ticker','per_30', 'per_90', 'per_180', 'per_240', 'per_365']]

df1_set_3 = pd.melt(df1_set_2, 
        # id_vars= ['date', 'ticker', 'corp_name'], 
        id_vars= ['date', 'ticker'], 
        value_vars=['per_30', 'per_90', 'per_180', 'per_240','per_365'])    
stock_ratio_per = df1_set_3.sort_values(by = ['ticker'])
stock_ratio_per.head()



# df_per_total = pd.DataFrame()
# per_set = ['per_30', 'per_90', 'per_180', 'per_240', 'per_365']
# ascending_list = [True, False]
# 
# for per_value in per_set:
#     for ascending_value in ascending_list:
#         df_per =  df1_set_2.sort_values(by = per_value, ascending = ascending_value).head()
#         df_per['type'] = per_value
#         # df_per['rank'] = range(1,6)
#         df_per['음/양'] = ascending_value
#         df_per_total = pd.concat([df_per_total, df_per])
# 
# df_per_total = df_per_total.drop_duplicates()     
# df_per_total = df_per_total.reset_index(drop = True)



date_nm = max(df_raw_anal_total_2['date'])
dfdf = df_raw_anal_total_2[df_raw_anal_total_2['date'] == date_nm]


# pd.wide_to_long(dfdf, stubnames='ht', i=['date', 'ticker','corp_name','market','close'], j='age')
dfdf2 = pd.melt(dfdf, 
        id_vars= ['date', 'ticker'], 
        value_vars=['5_20_cross', '20_60_cross', 'array', 'Bollinger_band','MACD','RSI'])
stock_indicator = dfdf2[dfdf2['value'] != '-']

df_total = pd.concat([stock_ratio_per,stock_indicator])
df_total['type'] = 'bitcoin'
df_total.columns = ['date', 'code', '등락률기간', '등락률', 'type']


df_total = df_total[['code','등락률기간', '등락률', 'type']]
df_total['date'] = date_nm





### gemini
api_key_df = pd.read_csv('key_value/chatgpt_apikey.csv')
GOOGLE_API_KEY = api_key_df[api_key_df['corp'] == 'google'].reset_index()['api_key'][0]

genai.configure(api_key=GOOGLE_API_KEY)

# Set up the model
generation_config = {
  "temperature": 0.9,
  "top_p": 1,
  "top_k": 1,
  "max_output_tokens": 2048,
}

model = genai.GenerativeModel('gemini-pro',
                             generation_config=generation_config)

# df_total = pd.read_csv('data_crawler/dashboard/indicator.csv')

total_df_1 = df_total[~df_total['등락률기간'].isin(['5_20_cross', '20_60_cross', 'array', 'Bollinger_band','MACD','RSI'])]
total_df_2 = df_total[df_total['등락률기간'].isin(['5_20_cross', '20_60_cross', 'array', 'Bollinger_band','MACD','RSI'])]



prompt = f"""
- 날짜:{today_date2} 
- 종목정보: 비트코인

- 등락률
    - {total_df_1}

- 매수/매도신호
    - {total_df_2}


증권 보고서 형태로 설명식으로 요약해줘. 
"""

try:
    response = model.generate_content(prompt)
    
    response_df = pd.DataFrame({'ticker':'bitcoin', 
                 'corp_name':'비트코인',
                 'date':today_date2,
                 'response_msg':response.text}, index = [0])
except:
    print('보고서 없음')
    response_df = pd.DataFrame({'ticker':'bitcoin', 
                 'corp_name':'비트코인',
                 'date':today_date2,
                 'response_msg':"증권 보고서 없음"}, index = [0])    

file_name = f'gemini_bitcoin'
now1 = datetime.now()
time_line = now1.strftime("%Y%m%d_%H:%M:%S")

try:
    # 빅쿼리 데이터 적재
    response_df.to_gbq(destination_table=f'{project_id}.{dataset_id}.{file_name}',
      project_id=project_id,
      if_exists='append',
      credentials=credentials)
    print(f'{file_name}_빅쿼리저장_success_{time_line}')
except:
    print(f'{file_name}_빅쿼리저장_fail_{time_line}')
    

if not os.path.exists(f'data_crawler/dashboard/gemini_result_bitcoin_{today_date1}.csv'):
    response_df.to_csv(f'data_crawler/dashboard/gemini_result_bitcoin_{today_date1}.csv', index=False, mode='w')
else:
    response_df.to_csv(f'data_crawler/dashboard/gemini_result_bitcoin_{today_date1}.csv', index=False, mode='a', header=False)

# Google Storage 적재
source_file_name = f'data_crawler/dashboard/gemini_result_bitcoin_{today_date1}.csv'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/dashboard/gemini_result_bitcoin_{today_date1}.csv'    # 업로드할 파일을 GCP에 저장할 때의 이름
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)


now1 = datetime.now()
time_line = now1.strftime("%Y%m%d_%H:%M:%S")
print(f'{ticker_nm} 완료_{time_line}')
