import pandas as pd
import os
import glob
import plotly.express as px
import plotly.graph_objects as go
import json
from plotly.subplots import make_subplots

# import math

import streamlit as st
from datetime import datetime

from st_files_connection import FilesConnection

from google.oauth2 import service_account
from google.cloud import bigquery



st.set_page_config(
    page_title="KOSPI",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)


with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    # Very Important Point
    st.secrets["gcp_service_account"]
)

client = bigquery.Client(credentials=credentials)



conn = st.experimental_connection('gcs', type=FilesConnection)
                      
kor_ticker_list = conn.read("finance-mlops-1/data_crawler/kor_ticker_list/kor_ticker_list_20230825.csv", 
                      input_format="csv", ttl=600)
                      

ticker_list = kor_ticker_list['corp_code'].unique()                      

option = st.selectbox(
    'How would you like to be contacted?',
    ticker_list)

st.write('You selected:', option)



@st.cache_data(ttl=600)
def run_query(query):
    # sql = f"SELECT {cols} FROM streamlit-dashboard-369600.seoul.{name}"
    # sql = f'SELECT * FROM `owenchoi-396200.finance_mlops.kor_stock_ohlcv`'
    # query
    df = client.query(query).to_dataframe()

    # st.dataframe(df)
    return df
    
kor_stock_ohlcv = run_query(f"""SELECT * 
                                FROM `owenchoi-396200.finance_mlops.kor_stock_ohlcv` 
                                where ticker = '{option}' AND date > '2020-01-01'""")


st.dataframe(df)