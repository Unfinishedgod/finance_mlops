#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import pandas_gbq
from pykrx import stock
from pykrx import bond
import FinanceDataReader as fdr
from datetime import timedelta

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
os.chdir('/home/shjj08choi/finance_mlops')

# 서비스 계정 키 JSON 파일 경로
key_path = glob.glob("key_value/*.json")[0]

# Credentials 객체 생성
credentials = service_account.Credentials.from_service_account_file(key_path)

# 빅쿼리 정보
project_id = 'owenchoi-396200'
dataset_id = 'finance_mlops'

# GCP 클라이언트 객체 생성
storage_client = storage.Client(credentials = credentials, 
                         project = credentials.project_id)
bucket_name = 'finance-mlops-1'     # 서비스 계정 생성한 bucket 이름 입력

# Postgresql 연결
db_connect_info = pd.read_csv('key_value/db_connect_info.csv')
username = db_connect_info['username'][0]
password = db_connect_info['password'][0]
host = db_connect_info['host'][0]
database = db_connect_info['database'][0]
engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:5432/{database}')


def upload_df(data, file_name, project_id, dataset_id, time_line, today_date1):
    if not os.path.exists(f'data_crawler/{file_name}'):
        os.makedirs(f'data_crawler/{file_name}')

    try:
        if not os.path.exists(f'data_crawler/{file_name}/{file_name}_{today_date1}.csv'):
            data.to_csv(f'data_crawler/{file_name}/{file_name}_{today_date1}.csv', index=False, mode='w')
        else:
            data.to_csv(f'data_crawler/{file_name}/{file_name}_{today_date1}.csv', index=False, mode='a', header=False)
        print(f'{file_name}_로컬CSV저장_success_{time_line}')    
    except:
        print(f'{file_name}_로컬CSV저장_fail_{time_line}')
    
    
    # Google Storage 적재
    source_file_name = f'data_crawler/{file_name}/{file_name}_{today_date1}.csv'    # GCP에 업로드할 파일 절대경로
    destination_blob_name = f'data_crawler/{file_name}/{file_name}_{today_date1}.csv'    # 업로드할 파일을 GCP에 저장할 때의 이름
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
        
        
        
# ### 날짜 설정
now = datetime.now()
now = now + timedelta(days=-1)

today_date1 = now.strftime('%Y%m%d')
today_date2 = now.strftime('%Y-%m-%d')
start_date2 = today_date2
# start_date2 = '2017-01-01'
today_date_time_csv = now.strftime("%Y%m%d_%H%M")

# ### S&P 500 종목 리스트 

# S&P 500 symbol list
snp500 = fdr.StockListing('S&P500')
snp500.columns = ['ticker', 'corp_name', 'sector', 'industry']

now1 = datetime.now()
time_line = now1.strftime("%Y%m%d_%H:%M:%S")  
file_name = 'snp500_ticker_list'
upload_df(snp500, file_name, project_id, dataset_id, time_line, today_date1)
sp500_ticker_list = snp500['ticker']

file_name = 'snp500_daily'
df_raw_total = pd.DataFrame()
for ticker_nm in sp500_ticker_list:
    try:
        now1 = datetime.now()
        
        time_line = now1.strftime("%Y%m%d_%H:%M:%S")
        time.sleep(1)
        # Apple(AAPL), 2017-01-01 ~ Now
        df_raw = fdr.DataReader(ticker_nm, start_date2,today_date2)
        df_raw['ticker'] = ticker_nm
        df_raw = df_raw.reset_index()
        df_raw.columns = ['date', 'open','high','low','close','adj_close','volume','ticker']

        df_raw_total = pd.concat([df_raw_total,df_raw])
        
        print(f'{ticker_nm} success_{time_line}')   
    except:
        print(f'{ticker_nm} fail_{time_line}')
        
df_raw_total['date'] = pd.to_datetime(df_raw_total['date'])
now1 = datetime.now()
time_line = now1.strftime("%Y%m%d_%H:%M:%S")
upload_df(df_raw_total, file_name, project_id, dataset_id, time_line, today_date1)    










# # ### 비트코인 
# 
# # In[16]:
# print('비트코인 시작')
# btc_df = fdr.DataReader('BTC/KRW', start_date2,today_date2)
# btc_df = btc_df.reset_index()
# 
# file_name = 'bitcoin'
# if not os.path.exists(f'{file_dir}/{file_name}_{today_date_time_csv}.csv'):
#     df_raw.to_csv(f'{file_dir}/{file_name}_{today_date_time_csv}.csv', index=False, mode='w')
# else:
#     df_raw.to_csv(f'{file_dir}/{file_name}_{today_date_time_csv}.csv', index=False, mode='a', header=False)
#  
# print('비트코인 완료')   



# In[17]:


# btc_df.to_csv(f'data_crawler/btc_df.csv')


# # ### 환율 정보
# 
# # In[19]:
# 
# 
# usdkrw = fdr.DataReader('USD/KRW', '1995-01-01', today_date2) # 달러 원화
# usdkrw = usdkrw.reset_index()
# 
# 
# # In[20]:
# 
# 
# usdkrw.to_csv(f'data_crawler/usdkrw.csv')
# 
# 
# # In[23]:
# 
# 
# usdkrw.tail()

