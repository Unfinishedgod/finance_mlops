#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import pandas_gbq
from pykrx import stock
from pykrx import bond
import FinanceDataReader as fdr


from time import sleep

import psycopg2 as pg2
from sqlalchemy import create_engine

from datetime import datetime
from datetime import timedelta

import os
import time

import glob
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage


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
bucket_name = 'finance-mlops-proj'     # 서비스 계정 생성한 bucket 이름 입력

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

start_date1 = '20180101'
today_date1 = '20240105'

start_date1 = '20240705'
today_date1 = '20250118'

def upload_df(data, file_name, project_id, dataset_id, time_line):
    if not os.path.exists(f'data_crawler/{file_name}'):
        os.makedirs(f'data_crawler/{file_name}')

    try:
        if not os.path.exists(f'data_crawler/{file_name}/{file_name}.csv'):
            data.to_csv(f'data_crawler/{file_name}/{file_name}.csv', index=False, mode='w')
        else:
            data.to_csv(f'data_crawler/{file_name}/{file_name}.csv', index=False, mode='a', header=False)
        print(f'{file_name}_로컬CSV저장_success_{time_line}')    
    except:
        print(f'{file_name}_로컬CSV저장_fail_{time_line}')
    
    
    # Google Storage 적재
    source_file_name = f'data_crawler/{file_name}/{file_name}.csv'    # GCP에 업로드할 파일 절대경로
    destination_blob_name = f'data_crawler/{file_name}/{file_name}.csv'    # 업로드할 파일을 GCP에 저장할 때의 이름
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

    try:
        # 빅쿼리 데이터 적재
        data.to_gbq(destination_table=f'{project_id}.{dataset_id}.{file_name}',
          project_id=project_id,
          if_exists='append',
          credentials=credentials)
        print(f'{file_name}_빅쿼리저장_success_{time_line}')
    except:
        print(f'{file_name}_빅쿼리저장_fail_{time_line}')


    try:
        # Postgresql 적재
        data.to_sql(f'{file_name}',if_exists='append', con=engine,  index=False)
        print(f'{file_name}_Postgresql저장_success_{time_line}')
    except:
        print(f'{file_name}_Postgresql저장_fail_{time_line}')

# # 주식 정보


## 티커 리스트
market_list = ['KOSPI', 'KOSDAQ']
kor_ticker_list_df = pd.DataFrame()
for market_nm in market_list:
    ticker_list = stock.get_market_ticker_list(today_date1, market=market_nm)
    for tickers in ticker_list:
        corp_name = stock.get_market_ticker_name(tickers)
        df = pd.DataFrame({'ticker':tickers,
                           'corp_name':corp_name,
                           'market': market_nm
                          }, index = [0])
        kor_ticker_list_df = pd.concat([kor_ticker_list_df,df])
kor_ticker_list_df = kor_ticker_list_df.reset_index(drop = True)


# 시가총액 별로 정렬
df_raw = stock.get_market_cap(today_date1,  market="ALL")
kor_ticker_list_df = kor_ticker_list_df.reset_index(drop = True)
kor_ticker_list_df_2 = pd.merge(kor_ticker_list_df, df_raw[['market_cap', 'ticker']],
        on = 'ticker', 
        how = 'left')
kor_ticker_list_df_2['rank'] = kor_ticker_list_df_2.groupby('market')['market_cap'].rank(method='min', ascending=False)
kor_ticker_list_df_2['rank'] = kor_ticker_list_df_2['rank'].astype(int)
kor_ticker_list_df_2 = kor_ticker_list_df_2.drop(['market_cap'], axis = 1)

kor_ticker_list_df = kor_ticker_list_df_2.sort_values(by = 'rank').reset_index(drop = True)


now1 = datetime.now()
time_line = now1.strftime("%Y%m%d_%H:%M:%S")  

file_name = 'kor_ticker_list'
upload_df(kor_ticker_list_df, file_name, project_id, dataset_id, time_line)
kor_ticker_list = kor_ticker_list_df['ticker']


# 주가 정보
print('주가정보 시작')
file_name = 'kor_stock_ohlcv'

for ticker_nm in kor_ticker_list:
    time.sleep(0.5)
    try:
        df_raw = stock.get_market_ohlcv(start_date1, today_date1, ticker_nm)
        df_raw = df_raw.reset_index()
        # df_raw['날짜'] = today_date2
        df_raw['티커'] = ticker_nm
        df_raw = df_raw[['날짜', '시가', '고가', '저가', '종가', '거래량', '등락률', '티커']]
        df_raw.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'price_change_percentage', 'ticker']

        df_raw['date'] = pd.to_datetime(df_raw['date'])


        now1 = datetime.now()
        time_line = now1.strftime("%Y%m%d_%H:%M:%S")

        upload_df(df_raw, file_name, project_id, dataset_id, time_line)
        print(f'주가정보 완료_{ticker_nm}_{time_line}')
    except:
        print(f'주가정보 실패_{ticker_nm}_{time_line}')

print(f'시가총액 시작')
file_name = 'kor_market_cap'
for ticker_nm in kor_ticker_list:
    time.sleep(0.5)
    try:
        df_raw = stock.get_market_cap(start_date1, today_date1, ticker_nm)
        df_raw = df_raw.reset_index()
        # df_raw['날짜'] = today_date2
        df_raw['티커'] = ticker_nm
        df_raw = df_raw[['날짜', '시가총액', '거래량','거래대금' ,'상장주식수', '티커']]
        df_raw.columns = ['date', 'market_cap', 'volume', 'trading_value', 'outstanding_shares', 'ticker']
        df_raw['date'] = pd.to_datetime(df_raw['date'])


        now1 = datetime.now()
        time_line = now1.strftime("%Y%m%d_%H:%M:%S")

        upload_df(df_raw, file_name, project_id, dataset_id, time_line)
        print(f'시가총액 완료_{ticker_nm}_{time_line}')
    except:
        print(f'시가총액 실패_{ticker_nm}_{time_line}')
      



# DIV/BPS/PER/EPS 조회
print(f'DIV/BPS/PER/EPS 시작')
file_name = 'kor_stock_fundamental'
for ticker_nm in kor_ticker_list:
    time.sleep(0.5)
    try:
        df_raw = stock.get_market_fundamental(start_date1, today_date1, ticker_nm)
        df_raw = df_raw.reset_index()
        # df_raw['날짜'] = today_date2
        df_raw['티커'] = ticker_nm
        df_raw = df_raw[['날짜', 'BPS', 'PER','PBR', 'EPS', 'DIV', 'DPS', '티커']]
        df_raw.columns = ['date', 'bps', 'per', 'pbr', 'eps', 'div', 'dps', 'ticker']
        df_raw['date'] = pd.to_datetime(df_raw['date'])
        
        now1 = datetime.now()
        time_line = now1.strftime("%Y%m%d_%H:%M:%S")
        
        upload_df(df_raw, file_name, project_id, dataset_id, time_line)
        print(f'DIV/BPS/PER/EPS 완료_{ticker_nm}_{time_line}')
    except:
        print(f'DIV/BPS/PER/EPS 실패_{ticker_nm}_{time_line}')
      
      
    

# # 인덱스 정보
print(f'인덱스 정보 시작')
kor_index_list_df = pd.DataFrame()
market_list = ['KOSPI', 'KOSDAQ']

for market_nm in market_list:
    kor_index_list = stock.get_index_ticker_list(market=market_nm)
    for index_codes in kor_index_list:
        index_name = stock.get_index_ticker_name(index_codes)
        df = pd.DataFrame({'index_code':index_codes,
                           'index_code_nm':index_name,
                           'market': market_nm
                          }, index = [0])
        kor_index_list_df = pd.concat([kor_index_list_df,df])

kor_index_list_df = kor_index_list_df.reset_index(drop = True)

file_name = 'kor_index_list_df'

now1 = datetime.now()
time_line = now1.strftime("%Y%m%d_%H:%M:%S")
upload_df(kor_index_list_df, file_name, project_id, dataset_id, time_line)



print(f'인덱스 정보 시작')

kor_index_code_list  = kor_index_list_df['index_code']


# ## 인덱스 OHLCV 조회
print(f'인덱스 OHLCV 시작')

file_name = 'kor_index_ohlcv'
df_raw_total = pd.DataFrame()

for index_code in kor_index_code_list:
    now1 = datetime.now()
    time_line = now1.strftime("%Y%m%d_%H:%M:%S")
    time.sleep(0.5)
    try:
        df_raw = stock.get_index_ohlcv(start_date1, today_date1, index_code)
        df_raw = df_raw.reset_index()
        df_raw['index_code'] = index_code
        df_raw.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'trading_value', 'market_cap', 'index_code']
        df_raw_total = pd.concat([df_raw_total,df_raw])

        print(f'{file_name}_{index_code}_데이터수집_success_{time_line}')
    except:
        print(f'{file_name}_{index_code}_데이터수집_fail')

df_raw_total['date'] = pd.to_datetime(df_raw_total['date'])

now1 = datetime.now()
time_line = now1.strftime("%Y%m%d_%H:%M:%S")

upload_df(df_raw_total, file_name, project_id, dataset_id, time_line)

print(f'인덱스 OHLCV 완료_{time_line}')




# 인덱스 등락률
print(f'인덱스 등락률 시작')
file_name = 'kor_index_code_fundamental'
df_raw_total = pd.DataFrame()

for index_code in kor_index_code_list:
    now1 = datetime.now()
    time_line = now1.strftime("%Y%m%d_%H:%M:%S")
    try:
        df_raw = stock.get_index_fundamental(start_date1, today_date1, index_code)
        df_raw = df_raw.reset_index()
        df_raw['index_code'] = index_code
        df_raw.columns = ['date', 'close', 'price_change_percentage', 'per', 'porward_per', 'pbr', 'dividend_yield', 'index_code']
        df_raw_total = pd.concat([df_raw_total,df_raw])
        print(f'{file_name}_{index_code}_데이터수집_success_{time_line}')
    except:
        print(f'{file_name}_{index_code}_데이터수집_fail')

df_raw_total['date'] = pd.to_datetime(df_raw_total['date'])

now1 = datetime.now()
time_line = now1.strftime("%Y%m%d_%H:%M:%S")

upload_df(df_raw_total, file_name, project_id, dataset_id, time_line)

print(f'인덱스 등락률 완료_{time_line}')




# 인덱스 구성 종목
print(f'인덱스 구성 종목 시작')

index_code_info = pd.DataFrame()
for index_code in kor_index_code_list:
    pdf = stock.get_index_portfolio_deposit_file(str(index_code))
    df = pd.DataFrame({'ticker':pdf,
                       'index_code': str(index_code)})
    index_code_info = pd.concat([index_code_info, df])
    
index_code_info = index_code_info.reset_index(drop = True)


index_code_info_2  = pd.merge(index_code_info, kor_index_list_df,
        how = 'left',
        on = 'index_code')


# kor_ticker_list_df = pd.read_csv(f'data_crawler/kor_ticker_list.csv')

index_code_master  = pd.merge(index_code_info_2, kor_ticker_list_df[['ticker','corp_name']],
        how = 'left',
        on = 'ticker')

file_name = 'index_code_master'



now1 = datetime.now()
time_line = now1.strftime("%Y%m%d_%H:%M:%S")

upload_df(index_code_master, file_name, project_id, dataset_id, time_line)

print(f'인덱스 구성 종목 완료_{time_line}')

