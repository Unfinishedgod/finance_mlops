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
    page_title="KOSDAQ",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)


with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)



conn = st.connection('gcs', type=FilesConnection)
                      
# parquet
kor_stock_ohlcv = conn.read("finance-mlops-proj/data_crawler/cleaning/kor_stock_ohlcv/kor_stock_ohlcv_kosdaq.parquet",
                      input_format="parquet", ttl=600)
kor_stock_ohlcv_anal = conn.read("finance-mlops-proj/data_crawler/cleaning/kor_stock_ohlcv/kor_stock_ohlcv_anal_kosdaq.parquet",
                      input_format="parquet", ttl=600)
                      
kor_stock_ohlcv = kor_stock_ohlcv.sort_values(by= ['date', 'rank'])



kor_ticker_list = kor_stock_ohlcv[kor_stock_ohlcv['market'] == 'KOSDAQ']
corp_name_list = kor_stock_ohlcv['corp_name'].unique()


col11, col22 = st.columns([1,3])
    
with col11:
    option = st.selectbox(
        'How would you like to be contacted?',
        corp_name_list)
        # ticker_list)
    st.write('You selected:', option)
with col22:
    asdf = st.radio(
        "Set label visibility 👇",
        ['5_20_cross', '20_60_cross', 'array', 'Bollinger_band', 'MACD', 'RSI', 'NONE'],
        horizontal=True
    )

ticker_nm_option = kor_stock_ohlcv[kor_stock_ohlcv['corp_name'] == option].reset_index(drop=True)['ticker'][0]


kor_stock_ohlcv_095570_total = kor_stock_ohlcv[kor_stock_ohlcv['ticker'] == ticker_nm_option].reset_index()
kor_stock_ohlcv_anal_total = kor_stock_ohlcv_anal[kor_stock_ohlcv_anal['ticker'] == ticker_nm_option].reset_index()
 
 
fig = functional.macd_vis(kor_stock_ohlcv_095570_total, kor_stock_ohlcv_anal_total,asdf, option)





col1, col2 = st.columns([3,1])

with col1:
  st.plotly_chart(fig, use_container_width=True)

with col2:
  st.write('asdf')
  # st.metric("PER", kor_stock_fundamental_total, kor_stock_fundamental_total)
  # st.dataframe(kor_stock_fundamental_total_df, hide_index=True)
