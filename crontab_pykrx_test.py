#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from pykrx import stock
from pykrx import bond
from time import sleep

from datetime import datetime
import os
import time

kor_ticker_list_df = pd.read_csv(f'kor_ticker_list.csv')
kor_ticker_list = kor_ticker_list_df['ticker']


# 현재 시간
now = datetime.datetime.now()
now_time = now.strftime('%Y-%m-%d %H:%M:%S')

for ticker_nm in kor_ticker_list:
    file_name = 'kor_stock_ohlcv'
    
    try:
        df_raw = stock.get_market_ohlcv(start_date, today_date1, ticker_nm)
        df_raw = df_raw.reset_index()
        df_raw['ticker'] = ticker_nm
        
        
        if not os.path.exists(f'data_crawler/{file_name}.csv'):
            df_raw.to_csv(f'data_crawler/{file_name}.csv', index=False, mode='w')
        else:
            df_raw.to_csv(f'data_crawler/{file_name}.csv', index=False, mode='a', header=False)
        
        print(f'{ticker_nm} success')
    except:
        print(f'{ticker_nm} fail')    
