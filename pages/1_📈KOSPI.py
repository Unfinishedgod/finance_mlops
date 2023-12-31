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
from datetime import timedelta

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



conn = st.connection('gcs', type=FilesConnection)
                      
# parquet
kor_stock_ohlcv = conn.read("finance-mlops-proj/data_crawler/cleaning/kor_stock_ohlcv/kor_stock_ohlcv_kospi.parquet",
                      input_format="parquet", ttl=600)
kor_stock_ohlcv_anal = conn.read("finance-mlops-proj/data_crawler/cleaning/kor_stock_ohlcv/kor_stock_ohlcv_anal_kospi.parquet",
                      input_format="parquet", ttl=600)
                      

kor_ticker_list = kor_stock_ohlcv[kor_stock_ohlcv['market'] == 'KOSPI']
corp_name_list = kor_stock_ohlcv['corp_name'].unique()

kor_stock_ohlcv = kor_stock_ohlcv.sort_values(by= ['date'])


col11, col22 = st.columns([1,3])
    
with col11:
    option = st.selectbox(
        'How would you like to be contacted?',
        corp_name_list)
        # ticker_list)
    ticker_nm_option = kor_stock_ohlcv[kor_stock_ohlcv['corp_name'] == option].reset_index(drop=True)['ticker'][0]        
    st.write('You selected:', ticker_nm_option)
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




  ################################################################################################
  ################################################################################################
  
  
  
  # parquet
  kor_index_ohlcv_cleaning = conn.read("finance-mlops-proj/data_crawler/cleaning/kor_index_ohlcv/kor_index_ohlcv_cleaning.parquet",
                        input_format="parquet", ttl=600)
  kor_index_list_df = conn.read("finance-mlops-proj/data_crawler/kor_index_list_df/kor_index_list_df.parquet",
                        input_format="parquet", ttl=600)
  index_code_master = conn.read("finance-mlops-proj/data_crawler/index_code_master/index_code_master.csv",
                        input_format="csv", ttl=600)
  
  # ### 날짜 설정
  now = datetime.now()
  now = now + timedelta(days=-30)
  
  today_date2 = now.strftime('%Y-%m-%d')
  
  kor_index_ohlcv_cleaning = kor_index_ohlcv_cleaning[kor_index_ohlcv_cleaning['date'] > today_date2]
  
  df = kor_index_ohlcv_cleaning.groupby(['index_code','index_code_nm'])['close'].apply(list).reset_index()
  
  index_list_df = index_code_master[index_code_master['index_code'] == ticker_nm_option].reset_index(drop = True)
  
  df_2 = df[df['index_code'].isin(index_list_df['index_code'])].reset_index(drop = True)
  
  st.dataframe(index_code_master)
  
  st.dataframe(
      df_2,
      column_config={
          "index_code": "App name",
          "index_name": "App index_name",
          "url": st.column_config.LinkColumn("App URL"),
          "close": st.column_config.LineChartColumn(
              "Views (past 30 days)", 
          ),
      },
      hide_index=True,
  )
  
  
  ################################################################################################
  ################################################################################################
