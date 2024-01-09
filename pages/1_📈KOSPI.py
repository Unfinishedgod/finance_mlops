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
                      input_format="parquet", ttl=3600)
kor_stock_ohlcv_anal = conn.read("finance-mlops-proj/data_crawler/cleaning/kor_stock_ohlcv/kor_stock_ohlcv_anal_kospi.parquet",
                      input_format="parquet", ttl=3600)

# parquet
kor_index_ohlcv_cleaning = conn.read("finance-mlops-proj/data_crawler/cleaning/kor_index_ohlcv/kor_index_ohlcv_cleaning.parquet",
                      input_format="parquet", ttl=3600)
kor_index_list_df = conn.read("finance-mlops-proj/data_crawler/kor_index_list_df/kor_index_list_df.parquet",
                      input_format="parquet", ttl=3600)
index_code_master = conn.read("finance-mlops-proj/data_crawler/index_code_master/index_code_master.csv",
                      input_format="csv", ttl=3600)
                      
index_code_master['index_code'] = index_code_master['index_code'].astype(str)
index_code_master['ticker'] = index_code_master['ticker'].astype(str).str.zfill(6)
  


kor_ticker_list = kor_stock_ohlcv[kor_stock_ohlcv['market'] == 'KOSPI']
corp_name_list = kor_stock_ohlcv['corp_name'].unique()
kor_stock_ohlcv = kor_stock_ohlcv.sort_values(by= ['date'])



now = datetime.now()
now = now + timedelta(days=-30)
today_date2 = now.strftime('%Y-%m-%d')
kor_index_ohlcv_cleaning = kor_index_ohlcv_cleaning[kor_index_ohlcv_cleaning['date'] >= today_date2]

not_sectors = ["1002","1003","1004","1028","1034","1035","1150","1151",
           "1152","1153","1154","1155","1156","1157","1158","1159",
           "1160","1167","1182","1224","1227","1232","1244","1894",
           "2002","2003","2004","2181","2182","2183","2184","2189",
           "2203","2212","2213","2214","2215","2216","2217","2218"]


  
  
  
  

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
        ['5_20_cross', '20_60_cross', 'Bollinger_band', 'MACD', 'RSI', 'NONE'],
        # ['5_20_cross', '20_60_cross', 'array', 'Bollinger_band', 'MACD', 'RSI', 'NONE'],
        horizontal=True
    )

ticker_nm_option = kor_stock_ohlcv[kor_stock_ohlcv['corp_name'] == option].reset_index(drop=True)['ticker'][0]
kor_stock_ohlcv_095570_total = kor_stock_ohlcv[kor_stock_ohlcv['ticker'] == ticker_nm_option].reset_index()
kor_stock_ohlcv_anal_total = kor_stock_ohlcv_anal[kor_stock_ohlcv_anal['ticker'] == ticker_nm_option].reset_index()



### index
index_code_master = index_code_master[~index_code_master['index_code'].isin(not_sectors)].reset_index(drop = True)
index_list_df = index_code_master[index_code_master['ticker'] == str(ticker_nm_option)].reset_index(drop = True)
dfdf22 = kor_index_ohlcv_cleaning[kor_index_ohlcv_cleaning['index_code'].isin(index_list_df['index_code'])].reset_index(drop = True)
  

now = datetime.now()
now = now + timedelta(days=-90)
today_date2 = now.strftime('%Y-%m-%d')
fig = functional.macd_vis(kor_stock_ohlcv_095570_total[kor_stock_ohlcv_095570_total['date'] > today_date2], 
                          kor_stock_ohlcv_anal_total[kor_stock_ohlcv_anal_total['date'] > today_date2], 
                          asdf, option)


fig2 = px.line(dfdf22, 
              x = 'date',
              y = 'close',
              line_shape="spline",
              facet_row="index_code_nm")
fig2.update_yaxes(matches=None)


message = """
**보고서**

**날짜:** 2024년 1월 5일

**주제:** 주식 시장 분석

**내용:**

삼성전자의 주가는 최근 30일 동안 4.21% 상승했고, 90일 동안은 12.64% 상승했으며, 180일 동안은 4.50% 상승했으며, 240일 동안은 17.12% 상승했으며, 365일 동안은 26.61% 상승했습니다. 해당 기간 동안 삼성전자는 정배열(매수)로 분석되었습니다.

코스피 지수는 최근 30일 동안 1.68% 상승했고, 90일 동안은 4.96% 상승했으며, 180일 동안은 -1.56% 하락했으며, 240일 동안은 3.94% 상승했으며, 365일 동안은 9.26% 상승했습니다. 코스피 지수의 MACD는 하향 돌파(매도)로 나타났습니다.

전기전자 산업의 주가는 최근 30일 동안 3.33% 상승했고, 90일 동안은 6.76% 상승했으며, 180일 동안은 -2.19% 하락했으며, 240일 동안은 9.32% 상승했으며, 365일 동안은 18.79% 상승했습니다.

제조업 산업의 주가는 최근 30일 동안 2.82% 상승했고, 90일 동안은 4.89% 상승했으며, 180일 동안은 -3.35% 하락했으며, 240일 동안은 4.38% 상승했으며, 365일 동안은 13.49% 상승했습니다.

**결론:**

전반적으로 삼성전자, 코스피 지수, 전기전자 산업, 제조업 산업의 주가는 최근 상승세를 보였습니다. 그러나 코스피 지수의 MACD가 하향 돌파로 나타나 앞으로 주가가 하락할 가능성이 있으므로 주의가 필요합니다."""



col1, col2 = st.columns([2,3])

with col1:
  st.markdown(message)
with col2:
  st.plotly_chart(fig, use_container_width=True)
  st.plotly_chart(fig2, use_container_width=True)  


