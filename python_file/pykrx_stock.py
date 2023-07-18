import pandas as pd
from pykrx import stock
from pykrx import bond
from time import sleep

from datetime import datetime
import os
import time


file_dir = '/home/owenchoi07/finance_mlops/data_crawler'

now = datetime.now()
today_date1 = now.strftime('%Y%m%d')
today_date2 = now.strftime('%Y-%m-%d')
today_date_time_csv = now.strftime("%Y%m%d_%H%M")

ticker_nm = '005930'

# 주가 정보 
print('주가정보 시작')
df_raw = stock.get_market_ohlcv(today_date1,  market="ALL")
df_raw = df_raw.reset_index()
df_raw = df_raw.rename(columns =  {'티커':'ticker'})
df_raw['날짜'] = today_date2


file_name = 'kor_stock_ohlcv'

if not os.path.exists(f'{file_dir}/{file_name}_{today_date_time_csv}.csv'):
    df_raw.to_csv(f'{file_dir}/{file_name}_{today_date_time_csv}.csv', index=False, mode='w')
else:
    df_raw.to_csv(f'{file_dir}/{file_name}_{today_date_time_csv}.csv', index=False, mode='a', header=False)
print('주가정보 완료')
    
    
print('시가총액 시작')
df_raw = stock.get_market_cap(today_date1,  market="ALL")
df_raw = df_raw.reset_index()
df_raw = df_raw.rename(columns =  {'티커':'ticker'})
df_raw['날짜'] = today_date2

file_name = 'kor_market_cap'
if not os.path.exists(f'{file_dir}/{file_name}_{today_date_time_csv}.csv'):
    df_raw.to_csv(f'{file_dir}/{file_name}_{today_date_time_csv}.csv', index=False, mode='w')
else:
    df_raw.to_csv(f'{file_dir}/{file_name}_{today_date_time_csv}.csv', index=False, mode='a', header=False)
 

print('시가총액 완료')   

# DIV/BPS/PER/EPS 조회 (매일 실행 되는 배치용)
print('DIV/BPS/PER/EPS 시작')

df_raw = stock.get_market_fundamental(today_date1, market='ALL')
df_raw = df_raw.reset_index()
df_raw.rename(columns = {'티커':'ticker'}, inplace = True)
df_raw['날짜'] = today_date2

file_name = 'kor_stock_fundamental'
if not os.path.exists(f'{file_dir}/{file_name}_{today_date_time_csv}.csv'):
    df_raw.to_csv(f'{file_dir}/{file_name}_{today_date_time_csv}.csv', index=False, mode='w')
else:
    df_raw.to_csv(f'{file_dir}/{file_name}_{today_date_time_csv}.csv', index=False, mode='a', header=False)

print('DIV/BPS/PER/EPS 완료')

