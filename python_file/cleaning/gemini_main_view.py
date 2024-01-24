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

import sys



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


# index_code_master = pd.read_csv('data_crawler/index_code_master/index_code_master.csv', dtype = {'ticker':str, 
#                                                                                                 'index_code':str})


sql = f"""
select * from `{project_id}.{dataset_id}.index_code_master`
"""

# 데이터 조회 쿼리 실행 결과
query_job = client.query(sql)

# 데이터프레임 변환
index_code_master = query_job.to_dataframe()


# not_sectors = ["1002","1003","1004","1028","1034","1035","1150","1151",
#            "1152","1153","1154","1155","1156","1157","1158","1159",
#            "1160","1167","1182","1224","1227","1232","1244","1894",
#            "2002","2003","2004","2181","2182","2183","2184","2189",
#            "2203","2212","2213","2214","2215","2216","2217","2218"]
# 
# index_code_master = index_code_master[~index_code_master['index_code'].isin(not_sectors)].reset_index(drop = True)


in_sectors = ['1001','2001']
index_code_master = index_code_master[index_code_master['index_code'].isin(in_sectors)].reset_index(drop = True)


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

df_total = pd.read_csv('data_crawler/dashboard/indicator.csv')

date_nm = df_total['date'].unique()



total_response_df = pd.DataFrame()
for ticker_nm in ['1001','2001']:
  
    df_total_kospi = df_total[df_total['code'] == ticker_nm]
    corp_nm = df_total_kospi['code_nm'].unique()
  
    prompt = f"""
    - 날짜:{today_date2} 
    - 종목정보: {corp_nm}

    - {df_total_kospi}

    증권 보고서 형태로 설명식으로 요약해줘. 
    """
    
    try:
        response = model.generate_content(prompt)
        response_df = pd.DataFrame({'ticker':ticker_nm,
                     'corp_name':corp_nm,
                     'date':today_date2,
                     'response_msg':response.text}, index = [0])

    except:
        print('증권 보고서 없음')
        response_df = pd.DataFrame({'ticker':ticker_nm,
                     'corp_name':corp_nm,
                     'date':today_date2,
                     'response_msg':"증권 보고서 없음"}, index = [0])

    # if not os.path.exists(f'data_crawler/dashboard/gemini_main_{today_date1}.csv'):
    #     response_df.to_csv(f'data_crawler/dashboard/gemini_main_{today_date1}.csv', index=False, mode='w')
    # else:
    #     response_df.to_csv(f'data_crawler/dashboard/gemini_main_{today_date1}.csv', index=False, mode='a', header=False)


    total_response_df = pd.concat([total_response_df, response_df])

kospi_msg = total_response_df[total_response_df['corp_name'] == '코스피'].reset_index(drop = True)['response_msg'][0]
kosdaq_msg = total_response_df[total_response_df['corp_name'] == '코스닥'].reset_index(drop = True)['response_msg'][0]

prompt = f"""
  - 날짜:{today_date2} 
  - 종목정보: 코스피&코스닥

  - {kospi_msg}
  - {kosdaq_msg} 

  증권 보고서 형태로 설명식으로 요약해줘. 
  """
  
try:
    response = model.generate_content(prompt)
    kospi_kosdaq_msg = pd.DataFrame({'ticker':ticker_nm,
                 'corp_name':corp_nm,
                 'date':today_date2,
                 'response_msg':response.text}, index = [0])

except:
    print('증권 보고서 없음')
    kospi_kosdaq_msg = pd.DataFrame({'ticker':ticker_nm,
                 'corp_name':corp_nm,
                 'date':today_date2,
                 'response_msg':"증권 보고서 없음"}, index = [0])
                   
                   
file_name = f'gemini_main_view'
now1 = datetime.now()
time_line = now1.strftime("%Y%m%d_%H:%M:%S")

try:
    # 빅쿼리 데이터 적재
    kospi_kosdaq_msg.to_gbq(destination_table=f'{project_id}.{dataset_id}.{file_name}',
      project_id=project_id,
      if_exists='append',
      credentials=credentials)
    print(f'{file_name}_빅쿼리저장_success_{time_line}')
except:
    print(f'{file_name}_빅쿼리저장_fail_{time_line}')


if not os.path.exists(f'data_crawler/dashboard/gemini_main_view_{today_date1}.csv'):
    kospi_kosdaq_msg.to_csv(f'data_crawler/dashboard/gemini_main_view_{today_date1}.csv', index=False, mode='w')
else:
    kospi_kosdaq_msg.to_csv(f'data_crawler/dashboard/gemini_main_view_{today_date1}.csv', index=False, mode='a', header=False)

# Google Storage 적재
source_file_name = f'data_crawler/dashboard/gemini_main_view_{today_date1}.csv'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/dashboard/gemini_main_view_{today_date1}.csv'    # 업로드할 파일을 GCP에 저장할 때의 이름
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)


now1 = datetime.now()
time_line = now1.strftime("%Y%m%d_%H:%M:%S")
print(f'완료_{time_line}')



os.system(f"python_file/send_gmail.py 'gemini_메인'")
 
