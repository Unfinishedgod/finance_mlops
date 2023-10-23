import pandas as pd
import os
import glob
import plotly.express as px
import plotly.graph_objects as go
import json
from plotly.subplots import make_subplots

import numpy as np

# import math

from ta.trend import MACD 
from ta.momentum import StochasticOscillator 

import streamlit as st
from datetime import datetime

from st_files_connection import FilesConnection

from google.oauth2 import service_account
from google.cloud import bigquery

import functional


st.set_page_config(
    page_title="test",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)


with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)


conn = st.experimental_connection('gcs', type=FilesConnection)
# kor_stock_ohlcv = conn.read("finance-mlops-1/data_crawler/kor_stock_ohlcv/kor_stock_ohlcv_20230825.csv",
#                       input_format="csv", ttl=600)
#                       
kor_ticker_list = conn.read("finance-mlops-1/data_crawler/kor_ticker_list/kor_ticker_list_20230825.csv",
                      input_format="csv", ttl=600)

kor_ticker_list = kor_ticker_list[kor_ticker_list['market'] == 'KOSPI']
ticker_list = kor_ticker_list['ticker'].unique()

option = st.selectbox(
    'How would you like to be contacted?',
    ticker_list)

st.write('You selected:', option)

ticker_nm = '095570'
df_raw = conn.read(f"finance-mlops-1/data_crawler/streamlit_data/kor_stock_ohlcv/{option}_20230925.csv",
                      input_format="csv", ttl=600)                      

# ticker_nm = '005930'
# start_date  = '20200101'
# today_date1 = '20231006'
# 
# df_raw = stock.get_market_ohlcv(start_date, today_date1, ticker_nm)
# df_raw = df_raw.reset_index()
# df_raw['ticker'] = ticker_nm
# 
# df_raw.columns = ['date', 'open', 'high', 'low', 'close', 'volume','trading_value','price_change_percentage', 'ticker']




df_raw['MA5'] = df_raw['close'].rolling(window=5).mean()
df_raw['MA20'] = df_raw['close'].rolling(window=20).mean()
df_raw['MA60'] = df_raw['close'].rolling(window=60).mean()
df_raw['MA120'] = df_raw['close'].rolling(window=120).mean()



std = df_raw['close'].rolling(20).std(ddof=0)

df_raw['upper'] = df_raw['MA20'] + 2 * std
df_raw['lower'] = df_raw['MA20'] - 2 * std



# MACD 
macd = MACD(close=df_raw['close'], 
            window_slow=26,
            window_fast=12, 
            window_sign=9)


df_raw['MACD_DIFF'] = macd.macd_diff()
df_raw['MACD'] = macd.macd()
df_raw['MACD_Signal'] = macd.macd_signal()



df_raw['변화량'] = df_raw['close'] - df_raw['close'].shift(1)
df_raw['상승폭'] = np.where(df_raw['변화량']>=0, df_raw['변화량'], 0)
df_raw['하락폭'] = np.where(df_raw['변화량'] <0, df_raw['변화량'].abs(), 0)

# welles moving average
df_raw['AU'] = df_raw['상승폭'].ewm(alpha=1/14, min_periods=14).mean()
df_raw['AD'] = df_raw['하락폭'].ewm(alpha=1/14, min_periods=14).mean()
df_raw['RSI'] = df_raw['AU'] / (df_raw['AU'] + df_raw['AD']) * 100



df_raw = df_raw[df_raw['date'] > '2023-01-01']
df_raw = df_raw.reset_index(drop = True)


down_reg = [idx for idx in range(1,len(df_raw)) if df_raw['RSI'][idx] > 70 and df_raw['RSI'][idx-1] <= 70]
top_reg = [idx for idx in range(1,len(df_raw)) if df_raw['RSI'][idx] < 30 and df_raw['RSI'][idx-1] >= 30]

down_reg_df = pd.DataFrame({
    'index':down_reg,
    'name':'매도'})

top_reg_df = pd.DataFrame({
    'index':top_reg,
    'name':'매수'})
    
cross_df = pd.concat([down_reg_df, top_reg_df])
cross_df = cross_df.reset_index(drop = True)



asdf = functional.macd_vis(df_raw)

st.plotly_chart(asdf, use_container_width=True)
