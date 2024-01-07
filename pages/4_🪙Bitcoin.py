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
    page_title="Bitcoin",
    page_icon="🪙",
    layout="wide",
    initial_sidebar_state="expanded"
)



with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)


conn = st.connection('gcs', type=FilesConnection)

option = 'bitcoin'
# parquet
bitcoin = conn.read("finance-mlops-proj/data_crawler/cleaning/bitcoin/bitcoin_cleaning.parquet",
                      input_format="parquet", ttl=600)
bitcoin_anal = conn.read("finance-mlops-proj/data_crawler/cleaning/bitcoin/bitcoin_anal_cleaning.parquet",
                      input_format="parquet", ttl=600)
                      
bitcoin = bitcoin.sort_values(by= 'date')


asdf = st.radio(
    "Set label visibility 👇",
    ['5_20_cross', '20_60_cross', 'array', 'Bollinger_band', 'MACD', 'RSI', 'NONE'],
    horizontal=True
)
    
fig = functional.macd_vis(bitcoin, bitcoin_anal,asdf, option)



col1, col2 = st.columns([3,1])

with col1:
  st.plotly_chart(fig, use_container_width=True)

with col2:
  st.write('asdf')
  # st.metric("PER", kor_stock_fundamental_total, kor_stock_fundamental_total)
  # st.dataframe(kor_stock_fundamental_total_df, hide_index=True)
