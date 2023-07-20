#!/usr/bin/env python
# coding: utf-8

# ##  FinanceDataReader 데이터 수집

# In[1]:


import pandas as pd
from time import sleep
import FinanceDataReader as fdr

from datetime import datetime
import os
import time


file_dir = '/home/owenchoi07/finance_mlops/data_crawler'


# ### S&P 500 종목 리스트 

# S&P 500 symbol list
snp500 = fdr.StockListing('S&P500')
snp500.columns = ['ticker', 'corp_name', 'sector', 'industry']
snp500.head()


snp50 = pd.read_csv(f'{file_dir}/snp500_ticker_list.csv')
sp500_ticker_list = snp500['ticker']


# In[4]:


# snp500.to_csv('data_crawler/snp500_ticker_list.csv', index = False)


# ### 날짜 설정
now = datetime.now()
now = now + timedelta(days=-1)

today_date1 = now.strftime('%Y%m%d')
today_date2 = now.strftime('%Y-%m-%d')
# start_date2 = '2017-01-01'
start_date2 = today_date2
today_date_time_csv = now.strftime("%Y%m%d_%H%M")

# ### S&P 500 데이터 수집


# In[8]:


for ticker_nm in sp500_ticker_list:
    file_name = 'snp500_daily'
    try:
        # Apple(AAPL), 2017-01-01 ~ Now
        df_raw = fdr.DataReader(ticker_nm, start_date2,today_date2)
        df_raw['ticker'] = ticker_nm
        df_raw = df_raw.reset_index()
        
        if not os.path.exists(f'{file_dir}/{file_name}_{today_date_time_csv}.csv'):
            df_raw.to_csv(f'{file_dir}/{file_name}_{today_date_time_csv}.csv', index=False, mode='w')
        else:
            df_raw.to_csv(f'{file_dir}/{file_name}_{today_date_time_csv}.csv', index=False, mode='a', header=False)
        
        print(f'{ticker_nm} success')    
    except:
        print(f'{ticker_nm} fail')


# In[10]:


df_raw.head()


# ### 비트코인 

# In[16]:
print('비트코인 시작')
btc_df = fdr.DataReader('BTC/KRW', start_date2,today_date2)
btc_df = btc_df.reset_index()

file_name = 'bitcoin'
if not os.path.exists(f'{file_dir}/{file_name}_{today_date_time_csv}.csv'):
    df_raw.to_csv(f'{file_dir}/{file_name}_{today_date_time_csv}.csv', index=False, mode='w')
else:
    df_raw.to_csv(f'{file_dir}/{file_name}_{today_date_time_csv}.csv', index=False, mode='a', header=False)
 
print('비트코인 완료')   



# In[17]:


# btc_df.to_csv(f'data_crawler/btc_df.csv')


# 
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

