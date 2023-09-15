import pandas as pd
import os
import glob
import plotly.express as px
import plotly.graph_objects as go
import json

# import math

import streamlit as st
from datetime import datetime

from st_files_connection import FilesConnection

st.set_page_config(
    page_title="Finance_mlops",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)






kor_index_ohlcv = pd.read_csv('data_crawler/kor_index_ohlcv/kor_index_ohlcv_20230825.csv', dtype = {'ticker': object})
kor_index_list_df = pd.read_csv('data_crawler/kor_index_list_df/kor_index_list_df_20230825.csv')



kor_index_ohlcv = kor_index_ohlcv[kor_index_ohlcv['date'] > '2023-07-01']

df = kor_index_ohlcv.groupby(['index_code'])['close'].apply(list).reset_index()


df2 = pd.merge(kor_index_list_df, df, 
        on = 'index_code', 
        how = 'left')
        
df_kospi = df2[df2['market'] == 'KOSPI']        
df_kosdaq = df2[df2['market'] == 'KOSDAQ']        
        
col1, col2 = st.columns(2)

with col1: 
    st.dataframe(
        df_kospi,
        column_config={
            "index_code": "App name",
            "index_name": "App index_name",
            "index_market": "App index_market",
            "url": st.column_config.LinkColumn("App URL"),
            "close": st.column_config.LineChartColumn(
                "Views (past 30 days)", 
            ),
        },
        hide_index=True,
    )

with col2: 
    st.dataframe(
      df_kosdaq,
      column_config={
          "index_code": "App name",
          "index_name": "App index_name",
          "index_market": "App index_market",
          "url": st.column_config.LinkColumn("App URL"),
          "close": st.column_config.LineChartColumn(
              "Views (past 30 days)",
          ),
      },
      hide_index=True,
  )
