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
    page_title="INDEX",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)


with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)



conn = st.connection('gcs', type=FilesConnection)
                      
# parquet
kor_index_ohlcv = conn.read("finance-mlops-proj/data_crawler/cleaning/kor_index_ohlcv/kor_index_ohlcv_cleaning.parquet",
                      input_format="parquet", ttl=600)
kor_index_ohlcv_anal = conn.read("finance-mlops-proj/data_crawler/cleaning/kor_index_ohlcv/kor_index_ohlcv_anal_cleaning.parquet",
                      input_format="parquet", ttl=600)
                      
kor_index_ohlcv = kor_index_ohlcv.sort_values(by= 'date')
kor_index_ohlcv_anal = kor_index_ohlcv_anal.sort_values(by= 'date')


index_code_nm_list = kor_index_ohlcv['index_code_nm'].unique()


col11, col22 = st.columns([1,3])
    
with col11:
    option = st.selectbox(
        'How would you like to be contacted?',
        index_code_nm_list)
        # index_code_list)
    st.write('You selected:', option)
with col22:
    asdf = st.radio(
        "Set label visibility 👇",
        ['5_20_cross', '20_60_cross', 'array', 'Bollinger_band', 'MACD', 'RSI', 'NONE'],
        horizontal=True
    )

index_code_nm_option = kor_index_ohlcv[kor_index_ohlcv['index_code_nm'] == option].reset_index(drop=True)['index_code'][0]


kor_index_ohlcv_095570_total = kor_index_ohlcv[kor_index_ohlcv['index_code'] == index_code_nm_option].reset_index()
kor_index_ohlcv_anal_total = kor_index_ohlcv_anal[kor_index_ohlcv_anal['index_code'] == index_code_nm_option].reset_index()
 
 
fig = functional.macd_vis(kor_index_ohlcv_095570_total, kor_index_ohlcv_anal_total,asdf, option)



st.plotly_chart(fig, use_container_width=True)


# 
# col1, col2 = st.columns([3,1])
# 
# with col1:
#   st.plotly_chart(fig, use_container_width=True)
# 
# with col2:
#   st.write('asdf')
#   # st.metric("PER", kor_stock_fundamental_total, kor_stock_fundamental_total)
#   # st.dataframe(kor_stock_fundamental_total_df, hide_index=True)
