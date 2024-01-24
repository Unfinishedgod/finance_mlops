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

from pyarrow import csv
import pyarrow as pa
import pyarrow.parquet as pq

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
bucket_name = 'finance-mlops-proj'    # 서비스 계정 생성한 bucket 이름 입력

# Postgresql 연결
db_connect_info = pd.read_csv('key_value/db_connect_info.csv')
username = db_connect_info['username'][0]
password = db_connect_info['password'][0]
host = db_connect_info['host'][0]
database = db_connect_info['database'][0]
engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:5432/{database}')

now = datetime.now()
# now = now + timedelta(days=-time_delta_nm)
today_date1 = now.strftime('%Y%m%d')
today_date2 = now.strftime('%Y-%m-%d')
today_date_time_csv = now.strftime("%Y%m%d_%H%M")


python_dir="/home/shjj08choi4/anaconda3/bin/python3"
file_dir="/home/shjj08choi4/finance_mlops/python_file"
dir1 = "/home/shjj08choi4/finance_mlops"

# for i in range(4):
#     os.system(f"nohup {python_dir} {file_dir}/cleaning/gemini_kospi.py {str(i)} >> gemini_kospi.txt &")
#     os.system(f"nohup {python_dir} {file_dir}/cleaning/gemini_kosdaq.py {str(i)} >> gemini_kosdaq.txt &")
# 
# final_i = 4
# 
# os.system(f"nohup {python_dir} {file_dir}/cleaning/gemini_kospi.py {str(final_i)} >> gemini_kospi.txt &")
# os.system(f"{python_dir} {file_dir}/cleaning/gemini_kosdaq.py {str(final_i)} >> gemini_kosdaq.txt")



gemini_result_kospi = pd.read_csv(f'data_crawler/dashboard/gemini_result_kospi_{today_date1}.csv')
gemini_result_kosdaq = pd.read_csv(f'data_crawler/dashboard/gemini_result_kosdaq_{today_date1}.csv')

gemini_result_kospi.to_csv(f'data_crawler/dashboard/gemini_result_kospi.csv')

table_from_pandas = pa.Table.from_pandas(gemini_result_kospi,preserve_index = False)
pq.write_table(table_from_pandas, f'data_crawler/dashboard/gemini_result_kospi.parquet')

table_from_pandas = pa.Table.from_pandas(gemini_result_kosdaq,preserve_index = False)
pq.write_table(table_from_pandas, f'data_crawler/dashboard/gemini_result_kosdaq.parquet')

# Google Storage 적재
source_file_name = f'data_crawler/dashboard/gemini_result_kospi.parquet'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/dashboard/gemini_result_kospi.parquet'    # 업로드할 파일을 GCP에 저장할 때의 이름
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)

# Google Storage 적재
source_file_name = f'data_crawler/dashboard/gemini_result_kosdaq.parquet'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/dashboard/gemini_result_kosdaq.parquet'    # 업로드할 파일을 GCP에 저장할 때의 이름
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)

os.system(f"python3 python_file/send_gmail.py 'gemini_코스피코스닥'")
