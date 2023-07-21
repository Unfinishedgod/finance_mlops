#!/usr/bin/env python
# coding: utf-8

# In[4]:


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


# import pydata_google_auth

# pandas_gbq
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


# Postgresql 연결
db_connect_info = pd.read_csv('key_value/db_connect_info.csv')

username = db_connect_info['username'][0]
password = db_connect_info['password'][0]
host = db_connect_info['host'][0]
database = db_connect_info['database'][0]

engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:5432/{database}')


# In[2]:


now = datetime.now()
today_date2 = now.strftime('%Y-%m-%d')
start_date2 = '2017-01-01'


# ## S&P 500 종목 리스트

# In[5]:


# S&P 500 symbol list
snp500 = fdr.StockListing('S&P500')
snp500.columns = ['ticker', 'corp_name', 'sector', 'industry']
snp500.head()


# In[7]:


file_name = 'snp500_ticker_list'
# 빅쿼리 데이터 적재
snp500.to_gbq(destination_table=f'{project_id}.{dataset_id}.{file_name}',
  project_id=project_id,
  if_exists='replace',
  credentials=credentials)

# Postgresql 적재
snp500.to_sql(f'{file_name}',if_exists='replace', con=engine,  index=False)



snp500.to_csv(f'data_crawler/{file_name}.csv', index=False, mode='w')


# Google Storage 적재
bucket_name = 'finance-mlops'    # 서비스 계정 생성한 bucket 이름 입력
source_file_name = f'data_crawler/{file_name}.csv'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/{file_name}/{file_name}.csv'    # 업로드할 파일을 GCP에 저장할 때의 이름

bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)

sp500_ticker_list = snp500['ticker']


# ## 주가 데이터 수집

# In[8]:


for ticker_nm in sp500_ticker_list:
    file_name = 'snp500'
    try:
        # Apple(AAPL), 2017-01-01 ~ Now
        df_raw = fdr.DataReader(ticker_nm, start_date2,today_date2)
        df_raw['ticker'] = ticker_nm
        df_raw = df_raw.reset_index()
        
        # 빅쿼리 데이터 적재
        df_raw.to_gbq(destination_table=f'{project_id}.{dataset_id}.{file_name}',
          project_id=project_id,
          if_exists='append',
          credentials=credentials)
        
        # Postgresql 적재
        df_raw.to_sql(f'{file_name}',if_exists='append', con=engine,  index=False)
        
        if not os.path.exists(f'data_crawler/{file_name}.csv'):
            df_raw.to_csv(f'data_crawler/{file_name}.csv', index=False, mode='w')
        else:
            df_raw.to_csv(f'data_crawler/{file_name}.csv', index=False, mode='a', header=False)
        
        print(f'{ticker_nm} success')
    except:
        print(f'{ticker_nm} fail')   

# Google Storage 적재        
bucket_name = 'finance-mlops'    # 서비스 계정 생성한 bucket 이름 입력
source_file_name = f'data_crawler/{file_name}.csv'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/{file_name}/{file_name}.csv'    # 업로드할 파일을 GCP에 저장할 때의 이름

bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)    


# ## 비트코인

# In[10]:


df_raw = fdr.DataReader('BTC/KRW', "2016-01-01",today_date2)
df_raw = df_raw.reset_index()

file_name = 'btc_df'

df_raw.to_csv(f'data_crawler/{file_name}.csv', index=False, mode='w')

# 빅쿼리 데이터 적재
df_raw.to_gbq(destination_table=f'{project_id}.{dataset_id}.{file_name}',
  project_id=project_id,
  if_exists='append',
  credentials=credentials)

# Postgresql 적재
df_raw.to_sql(f'{file_name}',if_exists='append', con=engine,  index=False)

# Google Storage 적재
bucket_name = 'finance-mlops'    # 서비스 계정 생성한 bucket 이름 입력
source_file_name = f'data_crawler/{file_name}.csv'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/{file_name}/{file_name}.csv'    # 업로드할 파일을 GCP에 저장할 때의 이름

bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)    


# ### 환율

# In[11]:


df_raw = fdr.DataReader('USD/KRW', "2016-01-01",today_date2)
df_raw = df_raw.reset_index()

file_name = 'usdkrw'

df_raw.to_csv(f'data_crawler/{file_name}.csv', index=False, mode='w')

# 빅쿼리 데이터 적재
df_raw.to_gbq(destination_table=f'{project_id}.{dataset_id}.{file_name}',
  project_id=project_id,
  if_exists='append',
  credentials=credentials)

# Postgresql 적재
df_raw.to_sql(f'{file_name}',if_exists='append', con=engine,  index=False)

# Google Storage 적재
bucket_name = 'finance-mlops'    # 서비스 계정 생성한 bucket 이름 입력
source_file_name = f'data_crawler/{file_name}.csv'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/{file_name}/{file_name}.csv'    # 업로드할 파일을 GCP에 저장할 때의 이름

bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)    


# In[ ]:




