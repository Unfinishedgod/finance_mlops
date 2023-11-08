import plotly.graph_objects as go

from ta.trend import MACD
from ta.momentum import StochasticOscillator

import numpy as np
import pandas as pd
from pykrx import stock
from pykrx import bond
from time import sleep

from datetime import datetime
from datetime import timedelta
import os
import time
from plotly.subplots import make_subplots
import glob

os.chdir('/home/shjj08choi4/finance_mlops')

now = datetime.now()
today_date1 = now.strftime('%Y%m%d')
start_date = '20180101'


kospi_100 = stock.get_index_portfolio_deposit_file("1034")



kor_ticker_list_df = pd.DataFrame()
ticker_list = kospi_100
for tickers in ticker_list:
    corp_name = stock.get_market_ticker_name(tickers)
    df = pd.DataFrame({'ticker':tickers,
                       'corp_name':corp_name,
                       'market': 'KOSPI_100'
                      }, index = [0])
    kor_ticker_list_df = pd.concat([kor_ticker_list_df,df])
kor_ticker_list_df = kor_ticker_list_df.reset_index(drop = True)


kor_ticker_list = kor_ticker_list_df['ticker']


ohlcv_df_raw = pd.DataFrame()
for ticker_nm in kor_ticker_list:
    try:
        df_raw = stock.get_market_ohlcv(start_date, today_date1, ticker_nm)
        df_raw = df_raw.reset_index()
        df_raw['ticker'] = ticker_nm
        
        ohlcv_df_raw = pd.concat([ohlcv_df_raw, df_raw])
        print(f'{ticker_nm} success')   
    except:
        print(f'{ticker_nm} fail')   
        
ohlcv_df_raw = ohlcv_df_raw.reset_index(drop = True)
ohlcv_df_raw.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'price_change_percentage', 'ticker']


ohlcv_df_raw = pd.merge(ohlcv_df_raw, kor_ticker_list_df, 
        on = 'ticker', 
        how = 'left')



ohlcv_df_raw.to_csv('data_crawler/ohlcv_df_raw.csv', index = False)
