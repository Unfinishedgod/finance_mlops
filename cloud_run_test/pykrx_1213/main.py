import functions_framework

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



@functions_framework.http
def hello_http(request):

    # 서비스 계정 키 JSON 파일 경로
    key_path = glob.glob("./*.json")[0]

    # Credentials 객체 생성
    credentials = service_account.Credentials.from_service_account_file(key_path)

    # 빅쿼리 정보
    project_id = 'owenchoi-404302'
    dataset_id = 'finance_mlops'

    # GCP 클라이언트 객체 생성
    storage_client = storage.Client(credentials = credentials,
                            project = credentials.project_id)
    bucket_name = 'finance-mlops-proj'    # 서비스 계정 생성한 bucket 이름 입력
      

    def upload_df(data, file_name, project_id, dataset_id, time_line, today_date1):
        if not os.path.exists(f'/tmp/{file_name}'):
            os.makedirs(f'/tmp/{file_name}')

        try:
            if not os.path.exists(f'/tmp/{file_name}/{file_name}_{today_date1}.csv'):
                data.to_csv(f'/tmp/{file_name}/{file_name}_{today_date1}.csv', index=False, mode='w')
            else:
                data.to_csv(f'/tmp/{file_name}/{file_name}_{today_date1}.csv', index=False, mode='a', header=False)
            print(f'{file_name}_로컬CSV저장_success_{time_line}')
        except:
            print(f'{file_name}_로컬CSV저장_fail_{time_line}')


        # Google Storage 적재
        source_file_name = f'/tmp/{file_name}/{file_name}_{today_date1}.csv'    # GCP에 업로드할 파일 절대경로
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


    now1 = datetime.now()


    today_date1 = '20231207'
    file_name = 'pykrxtest_222'
    time_line = now1.strftime("%Y%m%d_%H:%M:%S")

    df_raw = stock.get_market_ohlcv('20231206',  market="ALL")
#     df_raw = stock.get_market_ohlcv("20220720", "20231206", "005930")
    df_raw = df_raw.reset_index()

    df_raw.columns = ['date', 'oepn', 'high', 'low', 'close', 'volumn', 'ratio']
    print(len(df_raw))
    upload_df(df_raw, file_name, project_id, dataset_id, time_line, today_date1)

    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'name' in request_json:
        name = request_json['name']
    elif request_args and 'name' in request_args:
        name = request_args['name']
    else:
        name = 'World'
    return 'Hello {}!'.format(name)
