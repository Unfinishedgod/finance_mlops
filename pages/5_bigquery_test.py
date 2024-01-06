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


conn = st.connection('gcs', type=FilesConnection)


query_nm =  'SELECT * FROM `owenchoi-404302.finance_mlops.kor_stock_ohlcv`'

@st.cache_data(ttl=600)
# def run_query(query, name):
def run_query(query):
    st.write("Load DataFrame")
    # sql = f"SELECT {cols} FROM project_id 입력하는 부분.seoul.{name}"
    
    df = client.query(query).to_dataframe()



df = run_query(query_nm)

st.dataframe(df)
    
    
    
    
# asdf = functional.macd_vis(df_raw, ticker_nm)

# st.plotly_chart(asdf, use_container_width=True)
