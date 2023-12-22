import pandas as pd
import os
import glob
import plotly.express as px
import plotly.graph_objects as go
import json
from plotly.subplots import make_subplots

import functional
from ta.trend import MACD 
from ta.momentum import StochasticOscillator 
# import math

import streamlit as st
from datetime import datetime

from st_files_connection import FilesConnection
from pyarrow import csv
import pyarrow as pa
import pyarrow.parquet as pq


st.set_page_config(
    page_title="KOSPI",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)


with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)


# kor_index_ohlcv = pd.read_csv('data_crawler/kor_index_ohlcv/kor_index_ohlcv.csv', dtype = {'ticker': object})
# kor_index_list_df = pd.read_csv('data_crawler/kor_index_list_df/kor_index_list_df.csv')


# kor_stock_ohlcv = pd.read_csv('data_crawler/kor_stock_ohlcv/kor_stock_ohlcv.csv', dtype = {'ticker':object})
# kor_ticker_list = pd.read_csv('data_crawler/kor_ticker_list/kor_ticker_list.csv')

conn = st.connection('gcs', type=FilesConnection)
# kor_stock_ohlcv = conn.read("finance-mlops-proj/data_crawler/kor_stock_ohlcv/kor_stock_ohlcv.csv",
#                       input_format="csv", ttl=600)
# kor_ticker_list = conn.read("finance-mlops-proj/data_crawler/kor_ticker_list/kor_ticker_list.csv",
#                       input_format="csv", ttl=600)
#                       
# kor_stock_fundamental = conn.read("finance-mlops-proj/data_crawler/kor_stock_fundamental/kor_stock_fundamental.csv",
#                       input_format="csv", ttl=600)

                      
# parquet
kor_stock_ohlcv = conn.read("finance-mlops-proj/data_crawler/kor_stock_ohlcv/kor_stock_ohlcv_info_kospi.parquet",
                      input_format="parquet", ttl=600)                      
# kor_stock_ohlcv = kor_stock_ohlcv.to_pandas()

kor_ticker_list = conn.read("finance-mlops-proj/data_crawler/kor_ticker_list/kor_ticker_list.parquet",
                      input_format="parquet", ttl=600)
# kor_ticker_list = kor_ticker_list.to_pandas()
      
kor_stock_fundamental = conn.read("finance-mlops-proj/data_crawler/kor_stock_fundamental/kor_stock_fundamental_info_kospi.parquet",
                      input_format="parquet", ttl=600)
# kor_stock_fundamental = kor_stock_fundamental.to_pandas()



# kor_stock_ohlcv['MA120'] = kor_stock_ohlcv['close'].rolling(window=120).mean()
# kor_stock_ohlcv['MA60'] = kor_stock_ohlcv['close'].rolling(window=60).mean()
# kor_stock_ohlcv['MA20'] = kor_stock_ohlcv['close'].rolling(window=20).mean()
# kor_stock_ohlcv['MA5'] = kor_stock_ohlcv['close'].rolling(window=5).mean()
# 
# kor_stock_ohlcv = kor_stock_ohlcv[kor_stock_ohlcv['date'] > '2023-01-15']
# 
# 
# 
# # df1 = kor_stock_ohlcv[kor_stock_ohlcv['date'] == '2023-07-21']
# df1 = pd.merge(kor_stock_ohlcv, kor_ticker_list, 
#         on = 'ticker', 
#         how = 'left')
#         

kor_ticker_list = kor_ticker_list[kor_ticker_list['market'] == 'KOSPI']
ticker_list = kor_ticker_list['ticker'].unique()

option = st.selectbox(
    'How would you like to be contacted?',
    ticker_list)

st.write('You selected:', option)


ticker_nm = '095570'

kor_stock_fundamental_total = kor_stock_fundamental[kor_stock_fundamental['ticker'] == option].reset_index()



# kor_stock_ohlcv_095570_total = conn.read(f"finance-mlops-proj/data_crawler/streamlit_data/kor_stock_ohlcv/{option}_20230925.csv",
#                       input_format="csv", ttl=600)

kor_stock_ohlcv_095570_total = kor_stock_ohlcv[kor_stock_ohlcv['ticker'] == option].reset_index()


kor_stock_ohlcv_095570_total['MA5'] = kor_stock_ohlcv_095570_total['close'].rolling(window=5).mean()
kor_stock_ohlcv_095570_total['MA20'] = kor_stock_ohlcv_095570_total['close'].rolling(window=20).mean()
kor_stock_ohlcv_095570_total['MA60'] = kor_stock_ohlcv_095570_total['close'].rolling(window=60).mean()
kor_stock_ohlcv_095570_total['MA120'] = kor_stock_ohlcv_095570_total['close'].rolling(window=120).mean()

# MACD
kor_stock_ohlcv_095570_total['ema_short'] = kor_stock_ohlcv_095570_total['close'].rolling(window=12).mean()
kor_stock_ohlcv_095570_total['ema_long'] = kor_stock_ohlcv_095570_total['close'].rolling(window=26).mean()
kor_stock_ohlcv_095570_total['macd'] = kor_stock_ohlcv_095570_total['ema_short'] - kor_stock_ohlcv_095570_total['ema_long'] 


std = kor_stock_ohlcv_095570_total['close'].rolling(20).std(ddof=0)

kor_stock_ohlcv_095570_total['upper'] = kor_stock_ohlcv_095570_total['MA20'] + 2 * std
kor_stock_ohlcv_095570_total['lower'] = kor_stock_ohlcv_095570_total['MA20'] - 2 * std

 
fig = functional.func1(kor_stock_ohlcv_095570_total)


kor_stock_fundamental_total_df = kor_stock_fundamental_total[['bps', 'per', 'pbr', 'eps', 'div', 'dps']].T.reset_index()


col1, col2 = st.columns([3,1])

with col1:
  st.plotly_chart(fig, use_container_width=True)

with col2:
  # st.metric("PER", kor_stock_fundamental_total, kor_stock_fundamental_total)
  st.dataframe(kor_stock_fundamental_total_df, hide_index=True)
