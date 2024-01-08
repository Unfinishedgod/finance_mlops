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

today_date1 = '2024010'
today_date2 = '2023-01-07'

today_date1 = 'reset'



table_list = ['kor_index_code_fundamental',
'kor_index_ohlcv',
'kor_market_cap',
'kor_stock_fundamental',
'kor_stock_ohlcv',
'snp500_daily']



for table_nm in table_list:
    print(table_nm)
    
    sql = f"""
    select *
    from {table_nm}
    """
    df = pd.read_sql(sql, engine)

    # 빅쿼리 데이터 적재
    df.to_gbq(destination_table=f'{project_id}.{dataset_id}.{table_nm}',
      project_id=project_id,
      if_exists='append',
      credentials=credentials)
    
    print(f'{table_nm} 완료')
    time.sleep(10)
