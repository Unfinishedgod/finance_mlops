import os
import pandas as pd

import time
from time import sleep
from datetime import datetime
from datetime import timedelta

from pyarrow import csv
import pyarrow as pa
import pyarrow.parquet as pq

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage


now = datetime.now()
# now = now + timedelta(days=-2)
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



gemini_result_kospi = pd.read_csv(f'{dir1}/data_crawler/dashboard/gemini_result_kospi_{today_date1}.csv')
gemini_result_kosdaq = pd.read_csv(f'{dir1}/data_crawler/dashboard/gemini_result_kosdaq_{today_date1}.csv')


table_from_pandas = pa.Table.from_pandas(gemini_result_kospi,preserve_index = False)
pq.write_table(table_from_pandas, f'{dir1}/data_crawler/dashboard/gemini_result_kospi.parquet')

table_from_pandas = pa.Table.from_pandas(gemini_result_kosdaq,preserve_index = False)
pq.write_table(table_from_pandas, f'{dir1}/data_crawler/dashboard/gemini_result_kosdaq.parquet')

# Google Storage 적재
source_file_name = f'{dir1}/data_crawler/dashboard/gemini_result_kospi.parquet'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'{dir1}/data_crawler/dashboard/gemini_result_kospi.parquet'    # 업로드할 파일을 GCP에 저장할 때의 이름
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)

# Google Storage 적재
source_file_name = f'{dir1}/data_crawler/dashboard/gemini_result_kosdaq.parquet'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'{dir1}/data_crawler/dashboard/gemini_result_kosdaq.parquet'    # 업로드할 파일을 GCP에 저장할 때의 이름
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)

os.system(f"{file_dir}/send_gmail.py 'gemini_코스피코스닥'")
