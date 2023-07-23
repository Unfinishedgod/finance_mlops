#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import pandas_gbq
from pykrx import stock
from pykrx import bond
import FinanceDataReader as fdr

from time import sleep

import psycopg2 as pg2
from sqlalchemy import create_engine

from datetime import datetime
import os
import time

import glob
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage

# 경로 변경
os.chdir('/home/owenchoi07/finance_mlops')


# 서비스 계정 키 JSON 파일 경로
key_path = glob.glob("key_value/*.json")[0]

# Credentials 객체 생성
credentials = service_account.Credentials.from_service_account_file(key_path)

# 빅쿼리 정보
project_id = 'owen-389015'
dataset_id = 'finance_mlops'


# GCP 클라이언트 객체 생성
storage_client = storage.Client(credentials = credentials, 
                         project = credentials.project_id)
bucket_name = 'finance-mlops'    # 서비스 계정 생성한 bucket 이름 입력

# Postgresql 연결
db_connect_info = pd.read_csv('key_value/db_connect_info.csv')

username = db_connect_info['username'][0]
password = db_connect_info['password'][0]
host = db_connect_info['host'][0]
database = db_connect_info['database'][0]

engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:5432/{database}')


# In[2]:


now = datetime.now()
today_date1 = now.strftime('%Y%m%d')
start_date = '20230101'
today_date1 = '20230701'
# start_date = '20230701'


# # 주식 정보

# ## 티커 리스트
market_list = ['KOSPI', 'KOSDAQ', 'KONEX']

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

file_name = 'kor_ticker_list'

# 로컬 적재
kor_ticker_list_df.to_csv(f'data_crawler/{file_name}.csv', index=False, mode='w')
# 
# # 빅쿼리 데이터 적재
# kor_ticker_list_df.to_gbq(destination_table=f'{project_id}.{dataset_id}.{file_name}',
#   project_id=project_id,
#   if_exists='replace',
#   credentials=credentials)
# 
# # Postgresql 적재
# kor_ticker_list_df.to_sql(f'{file_name}',if_exists='replace', con=engine,  index=False)
# 
# # Google Storage 적재
# source_file_name = f'data_crawler/{file_name}.csv'    # GCP에 업로드할 파일 절대경로
# destination_blob_name = f'data_crawler/{file_name}/{file_name}.csv'    # 업로드할 파일을 GCP에 저장할 때의 이름
# 
# bucket = storage_client.bucket(bucket_name)
# blob = bucket.blob(destination_blob_name)
# blob.upload_from_filename(source_file_name)

kor_ticker_list = kor_ticker_list_df['ticker']


# # 인덱스 정보
# ## 인덱스 리스트
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

# 빅쿼리 데이터 적재
kor_index_list_df.to_gbq(destination_table=f'{project_id}.{dataset_id}.{file_name}',
  project_id=project_id,
  if_exists='replace',
  credentials=credentials)

# Postgresql 적재
kor_index_list_df.to_sql(f'{file_name}',if_exists='replace', con=engine,  index=False)
kor_index_list_df.to_csv(f'data_crawler/{file_name}.csv', index=False, mode='w')

# Google Storage 적재
source_file_name = f'data_crawler/{file_name}.csv'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/{file_name}/{file_name}.csv'    # 업로드할 파일을 GCP에 저장할 때의 이름

bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)


kor_index_code_list  = kor_index_list_df['index_code']


# ## 인덱스 OHLCV 조회
file_name = 'kor_index_ohlcv'
for ticker_nm in kor_ticker_list:
    df_raw = stock.get_index_ohlcv(start_date, today_date1, index_code)
    df_raw = df_raw.reset_index()
    df_raw['index_code'] = index_code
    df_raw.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'trading_value', 'market_cap', 'index_code']


    try:
        if not os.path.exists(f'data_crawler/{file_name}.csv'):
            df_raw.to_csv(f'data_crawler/{file_name}.csv', index=False, mode='w')
        else:
            df_raw.to_csv(f'data_crawler/{file_name}.csv', index=False, mode='a', header=False)
        print(f'{file_name}_{ticker_nm}_로컬CSV저장_success')    
    except:
        print(f'{file_name}_{ticker_nm}_로컬CSV저장_fail')
    
    try:
        # 빅쿼리 데이터 적재
        df_raw.to_gbq(destination_table=f'{project_id}.{dataset_id}.{file_name}',
          project_id=project_id,
          if_exists='append',
          credentials=credentials)
        print(f'{file_name}_{ticker_nm}_빅쿼리저장_success')    
    except:
        print(f'{file_name}_{ticker_nm}_빅쿼리저장_fail')  
    
    
    try:
        # Postgresql 적재
        df_raw.to_sql(f'{file_name}',if_exists='append', con=engine,  index=False)
        print(f'{file_name}_{ticker_nm}_Postgresql저장_success')    
    except:
        print(f'{file_name}_{ticker_nm}_Postgresql저장_fail')
        
        
# Google Storage 적재
source_file_name = f'data_crawler/{file_name}.csv'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/{file_name}/{file_name}.csv'    # 업로드할 파일을 GCP에 저장할 때의 이름

bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)        


# ## 인덱스 등락률
file_name = 'kor_index_code_fundamental'
for ticker_nm in kor_ticker_list:
    df_raw = stock.get_index_fundamental(start_date, today_date1, index_code)
    df_raw = df_raw.reset_index()
    df_raw['index_code'] = index_code
    df_raw.columns = ['date', 'close', 'price_change_percentage', 'per', 'porward_per', 'pbr', 'dividend_yield', 'index_code']

    try:
        if not os.path.exists(f'data_crawler/{file_name}.csv'):
            df_raw.to_csv(f'data_crawler/{file_name}.csv', index=False, mode='w')
        else:
            df_raw.to_csv(f'data_crawler/{file_name}.csv', index=False, mode='a', header=False)
        print(f'{file_name}_{ticker_nm}_로컬CSV저장_success')    
    except:
        print(f'{file_name}_{ticker_nm}_로컬CSV저장_fail')
    
    try:
        # 빅쿼리 데이터 적재
        df_raw.to_gbq(destination_table=f'{project_id}.{dataset_id}.{file_name}',
          project_id=project_id,
          if_exists='append',
          credentials=credentials)
        print(f'{file_name}_{ticker_nm}_빅쿼리저장_success')    
    except:
        print(f'{file_name}_{ticker_nm}_빅쿼리저장_fail')  
    
    
    try:
        # Postgresql 적재
        df_raw.to_sql(f'{file_name}',if_exists='append', con=engine,  index=False)
        print(f'{file_name}_{ticker_nm}_Postgresql저장_success')    
    except:
        print(f'{file_name}_{ticker_nm}_Postgresql저장_fail')
        
# Google Storage 적재
source_file_name = f'data_crawler/{file_name}.csv'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/{file_name}/{file_name}.csv'    # 업로드할 파일을 GCP에 저장할 때의 이름

bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)        



# ## 인덱스 구성 종목
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
        

kor_ticker_list_df = pd.read_csv(f'data_crawler/kor_ticker_list.csv')

index_code_master  = pd.merge(index_code_info_2, kor_ticker_list_df[['ticker','corp_name']],
        how = 'left',
        on = 'ticker')
        
file_name = 'index_code_master'

# 빅쿼리 데이터 적재
index_code_master.to_gbq(destination_table=f'{project_id}.{dataset_id}.{file_name}',
  project_id=project_id,
  if_exists='replace',
  credentials=credentials)

# Postgresql 적재
index_code_master.to_sql(f'{file_name}',if_exists='replace', con=engine,  index=False)


index_code_master.to_csv(f'data_crawler/{file_name}.csv', index=False, mode='w')

# Google Storage 적재
source_file_name = f'data_crawler/{file_name}.csv'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/{file_name}/{file_name}.csv'    # 업로드할 파일을 GCP에 저장할 때의 이름

bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)
