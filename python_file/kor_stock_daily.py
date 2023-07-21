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
today_date1 = now.strftime('%Y%m%d')
today_date2 = now.strftime('%Y-%m-%d')
today_date_time_csv = now.strftime("%Y%m%d_%H%M")


# # 주식 정보

# ## 티커 리스트

# In[3]:
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
# 빅쿼리 데이터 적재
kor_ticker_list_df.to_gbq(destination_table=f'{project_id}.{dataset_id}.{file_name}',
  project_id=project_id,
  if_exists='replace',
  credentials=credentials)

# Postgresql 적재
kor_ticker_list_df.to_sql(f'{file_name}',if_exists='replace', con=engine,  index=False)
        
kor_ticker_list_df.to_csv(f'data_crawler/{file_name}.csv', index=False, mode='w')

bucket_name = 'finance-mlops'    # 서비스 계정 생성한 bucket 이름 입력
source_file_name = f'data_crawler/{file_name}.csv'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/{file_name}/{file_name}.csv'    # 업로드할 파일을 GCP에 저장할 때의 이름

bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)

kor_ticker_list = kor_ticker_list_df['ticker']


# In[ ]:





# ## 종목별 주가 정보

# In[7]:


for ticker_nm in kor_ticker_list:
    file_name = 'kor_stock_ohlcv'
    
    try:
        df_raw = stock.get_market_ohlcv(start_date, today_date1, ticker_nm)
        df_raw = df_raw.reset_index()
        df_raw['ticker'] = ticker_nm
        df_raw.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'trading_value', 'price_change_percentage', 'ticker']

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


bucket_name = 'finance-mlops'    # 서비스 계정 생성한 bucket 이름 입력
source_file_name = f'data_crawler/{file_name}.csv'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/{file_name}/{file_name}.csv'    # 업로드할 파일을 GCP에 저장할 때의 이름

bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)

blob.upload_from_filename(source_file_name)        


# ## 종목별 시가총액

# In[8]:


df_raw = stock.get_market_cap('2023-07-11', today_date1, ticker_nm)
df_raw = df_raw.drop(['거래량', '거래대금'], axis = 1)
df_raw.head()


# In[9]:


for ticker_nm in kor_ticker_list:
    file_name = 'kor_market_cap'
    
    try:
        df_raw = stock.get_market_cap(start_date, today_date1, ticker_nm)
        df_raw = df_raw.reset_index()
        df_raw['ticker'] = ticker_nm
        df_raw = df_raw.drop(['거래량', '거래대금'], axis = 1)
        df_raw.columns = ['date', 'market_cap', 'outstanding_shares', 'ticker']
        
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

bucket_name = 'finance-mlops'    # 서비스 계정 생성한 bucket 이름 입력
source_file_name = f'data_crawler/{file_name}.csv'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/{file_name}/{file_name}.csv'    # 업로드할 파일을 GCP에 저장할 때의 이름

bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)

blob.upload_from_filename(source_file_name)        


# ## 종목별 DIV/BPS/PER/EPS

# In[10]:


for ticker_nm in kor_ticker_list:
    file_name = 'kor_stock_fundamental'
    
    try:
        df_raw = stock.get_market_fundamental(start_date, today_date1, ticker_nm)
        df_raw = df_raw.reset_index()
        df_raw['ticker'] = ticker_nm
        df_raw.columns = ['date', 'bps', 'per', 'pbr', 'eps', 'div', 'dps', 'ticker']
        
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

bucket_name = 'finance-mlops'    # 서비스 계정 생성한 bucket 이름 입력
source_file_name = f'data_crawler/{file_name}.csv'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/{file_name}/{file_name}.csv'    # 업로드할 파일을 GCP에 저장할 때의 이름

bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)

blob.upload_from_filename(source_file_name)        


# ##  일자별 거래실적 추이 (거래대금)

# In[11]:


buy_sell_type_list = ['순매수', '매수', '매도']
for buy_sell_type in buy_sell_type_list:
    for ticker_nm in kor_ticker_list:
        file_name = 'kor_stock_trading_value_by_investor'
        try:
            df_raw = stock.get_market_trading_value_by_date(start_date, today_date1, 
                                                                             ticker_nm, 
                                                                             detail=True,
                                                                             on = buy_sell_type)
            df_raw = df_raw.reset_index()
            df_raw['ticker'] = ticker_nm
            df_raw['type'] = buy_sell_type
            df_raw.columns = [
                'date', 
                'financial_investment', 'insurance', 'investment', 'private_equity', 'bank','other_finance', 'pension_fund', # 기관합계 
                'other_corporation', # 기타 법인
                'individual',# 개인
                'foreigner', 'other_foreigner', # 외국인 합계
                'total', 
                'ticker', 'type'
            ]

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
#     time.sleep(300)


bucket_name = 'finance-mlops'    # 서비스 계정 생성한 bucket 이름 입력
source_file_name = f'data_crawler/{file_name}.csv'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/{file_name}/{file_name}.csv'    # 업로드할 파일을 GCP에 저장할 때의 이름

bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)

blob.upload_from_filename(source_file_name)


# ### 일자별 거래실적 추이 (거래량)

# In[12]:


buy_sell_type_list = ['순매수', '매수', '매도']
for buy_sell_type in buy_sell_type_list:
    for ticker_nm in kor_ticker_list:
        file_name = 'kor_stock_trading_volume_by_date'
        try:
            df_raw = stock.get_market_trading_volume_by_date(start_date, today_date1, 
                                                                             ticker_nm, 
                                                                             detail=True,
                                                                             on = buy_sell_type)
            df_raw = df_raw.reset_index()
            df_raw['ticker'] = ticker_nm
            df_raw['type'] = buy_sell_type
            df_raw.columns = [
                'date', 
                'financial_investment', 'insurance', 'investment', 'private_equity', 'bank','other_finance', 'pension_fund', # 기관합계 
                'other_corporation', # 기타 법인
                'individual',# 개인
                'foreigner', 'other_foreigner', # 외국인 합계
                'total', 
                'ticker', 'type'
            ]

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
#     time.sleep(300)


bucket_name = 'finance-mlops'    # 서비스 계정 생성한 bucket 이름 입력
source_file_name = f'data_crawler/{file_name}.csv'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/{file_name}/{file_name}.csv'    # 업로드할 파일을 GCP에 저장할 때의 이름

bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)

blob.upload_from_filename(source_file_name)


# In[13]:


df_raw = stock.get_market_trading_volume_by_date(start_date, today_date1, 
                                                                 ticker_nm, 
                                                                 detail=True,
                                                                 on = buy_sell_type)
df_raw = df_raw.reset_index()
df_raw['ticker'] = ticker_nm
df_raw['type'] = buy_sell_type


# # 인덱스 정보
# 
# ## 인덱스 리스트

# In[15]:


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
kor_index_list_df.head()


file_name = 'kor_index_list_df'
# 빅쿼리 데이터 적재
kor_index_list_df.to_gbq(destination_table=f'{project_id}.{dataset_id}.{file_name}',
  project_id=project_id,
  if_exists='replace',
  credentials=credentials)

# Postgresql 적재
kor_index_list_df.to_sql(f'{file_name}',if_exists='replace', con=engine,  index=False)

kor_index_list_df.head()
kor_index_list_df.to_csv(f'data_crawler/{file_name}.csv', index=False, mode='w')


bucket_name = 'finance-mlops'    # 서비스 계정 생성한 bucket 이름 입력
source_file_name = f'data_crawler/{file_name}.csv'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/{file_name}/{file_name}.csv'    # 업로드할 파일을 GCP에 저장할 때의 이름

bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)


kor_index_code_list  = kor_index_list_df['index_code']


# ## 인덱스 OHLCV 조회

# In[16]:


for index_code in kor_index_code_list:
    file_name = 'kor_index_ohlcv'
    
    try:
        df_raw = stock.get_index_ohlcv(start_date, today_date1, index_code)
        df_raw = df_raw.reset_index()
        df_raw['index_code'] = index_code
        df_raw.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'trading_value', 'market_cap', 'index_code']

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
        
        print(f'{index_code} success')
    except:
        print(f'{index_code} fail')   
        
bucket_name = 'finance-mlops'    # 서비스 계정 생성한 bucket 이름 입력
source_file_name = f'data_crawler/{file_name}.csv'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/{file_name}/{file_name}.csv'    # 업로드할 파일을 GCP에 저장할 때의 이름

bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)        


# ## 인덱스 등락률

# In[17]:


for index_code in kor_index_code_list:
    file_name = 'kor_index_code_fundamental'
    
    try:
        df_raw = stock.get_index_fundamental(start_date, today_date1, index_code)
        df_raw = df_raw.reset_index()
        df_raw['index_code'] = index_code
        df_raw.columns = ['date', 'close', 'price_change_percentage', 'per', 'porward_per', 'pbr', 'dividend_yield', 'index_code']

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
        
        print(f'{index_code} success')
    except:
        print(f'{index_code} fail')   
        
bucket_name = 'finance-mlops'    # 서비스 계정 생성한 bucket 이름 입력
source_file_name = f'data_crawler/{file_name}.csv'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/{file_name}/{file_name}.csv'    # 업로드할 파일을 GCP에 저장할 때의 이름

bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)        


# ## 인덱스 구성 종목

# In[18]:


index_code_info = pd.DataFrame()
for index_code in kor_index_code_list:
    pdf = stock.get_index_portfolio_deposit_file(str(index_code))
    df = pd.DataFrame({'ticker':pdf,
                       'index_code': str(index_code)})
    index_code_info = pd.concat([index_code_info, df])
index_code_info = index_code_info.reset_index(drop = True)


# In[19]:


index_code_info_2  = pd.merge(index_code_info, kor_index_list_df,
        how = 'left',
        on = 'index_code')
        
index_code_info_2.head()


# In[20]:


kor_ticker_list_df = pd.read_csv(f'data_crawler/kor_ticker_list.csv')
kor_ticker_list_df.head()


# In[21]:


index_code_master  = pd.merge(index_code_info_2, kor_ticker_list_df[['ticker','corp_name']],
        how = 'left',
        on = 'ticker')
        
index_code_master.head()

file_name = 'index_code_master'
# 빅쿼리 데이터 적재
index_code_master.to_gbq(destination_table=f'{project_id}.{dataset_id}.{file_name}',
  project_id=project_id,
  if_exists='replace',
  credentials=credentials)

# Postgresql 적재
index_code_master.to_sql(f'{file_name}',if_exists='replace', con=engine,  index=False)

index_code_master.head()
index_code_master.to_csv(f'data_crawler/{file_name}.csv', index=False, mode='w')


bucket_name = 'finance-mlops'    # 서비스 계정 생성한 bucket 이름 입력
source_file_name = f'data_crawler/{file_name}.csv'    # GCP에 업로드할 파일 절대경로
destination_blob_name = f'data_crawler/{file_name}/{file_name}.csv'    # 업로드할 파일을 GCP에 저장할 때의 이름

bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)

blob.upload_from_filename(source_file_name)


# In[ ]:




