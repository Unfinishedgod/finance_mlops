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
import os
import time

import glob
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage

from crawler_func import upload_df

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

        
        
        
        
        
        
        
        
# now = datetime.now()
# today_date1 = now.strftime('%Y%m%d')
# start_date = '20180101'
# today_date1 = '20230721'
now = datetime.now()
# now = now + timedelta(days=-days_value)
today_date1 = now.strftime('%Y%m%d')
today_date2 = now.strftime('%Y-%m-%d')
today_date_time_csv = now.strftime("%Y%m%d_%H%M")



now1 = datetime.now()
time_line = now1.strftime("%Y%m%d_%H:%M:%S")  
# 
# ## 티커 리스트
# market_list = ['KOSPI', 'KOSDAQ']
# kor_ticker_list_df = pd.DataFrame()
# for market_nm in market_list:
#     ticker_list = stock.get_market_ticker_list(today_date1, market=market_nm)
#     for tickers in ticker_list:
#         corp_name = stock.get_market_ticker_name(tickers)
#         df = pd.DataFrame({'ticker':tickers,
#                            'corp_name':corp_name,
#                            'market': market_nm
#                           }, index = [0])
#         kor_ticker_list_df = pd.concat([kor_ticker_list_df,df])
# kor_ticker_list_df = kor_ticker_list_df.reset_index(drop = True)

file_name = 'asdf'

        

df = pd.DataFrame({'a':'b'}, index = [0])
upload_df(df, file_name, project_id, dataset_id, time_line, today_date1)
