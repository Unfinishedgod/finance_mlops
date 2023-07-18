#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd
from pykrx import stock
from pykrx import bond
from time import sleep

from datetime import datetime
import os
import time

import glob


# In[7]:


now = datetime.now()
today_date1 = now.strftime('%Y%m%d')
today_date2 = now.strftime('%Y-%m-%d')
ticker_nm = '005930'

file_dir = '/home/owenchoi07/finance_mlops/data_crawler'


# In[8]:


glob.glob(f'{file_dir}/*{today_date1}.csv')


# In[44]:


kor_ticker_list = pd.read_csv(f'{file_dir}/kor_ticker_list.csv')


# In[9]:


file_list = glob.glob(f'{file_dir}/*{today_date1}.csv')


# In[45]:


# df = pd.read_csv(file_list[1])
df = pd.read_csv(f'{file_dir}/kor_stock_ohlcv_20230717_1007.csv')


# In[60]:


df2 = pd.merge(df, kor_ticker_list,
        how = 'left',
        on = 'ticker')


# In[71]:

print('최대 최소 시작')
max_value_1 = df2[df2['market'] == 'KOSPI'].sort_values(by ='등락률', ascending = False).head(3)
max_value_2 = df2[df2['market'] == 'KOSDAQ'].sort_values(by ='등락률', ascending = False).head(3)
min_value_1 = df2[df2['market'] == 'KOSPI'].sort_values(by ='등락률', ascending = True).head(3)
min_value_2 = df2[df2['market'] == 'KOSDAQ'].sort_values(by ='등락률', ascending = True).head(3)


# In[78]:


min_max_stock = pd.concat([max_value_1,
          max_value_2,
          min_value_1,
          min_value_2])

# In[79]:


min_max_stock.to_csv(f'{file_dir}/min_max_{today_date1}.csv', index = False)

print('최대 최소 완료')
