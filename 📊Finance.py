import pandas as pd
import os
import glob
import plotly.express as px
import plotly.graph_objects as go
import json
from plotly.subplots import make_subplots
from datetime import datetime
from datetime import timedelta
# 
# from google.oauth2 import service_account
# from google.cloud import bigquery


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




with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)



# ### 날짜 설정
now = datetime.now()
now = now + timedelta(days=-30)

today_date2 = now.strftime('%Y-%m-%d')





# kor_index_ohlcv = pd.read_csv('data_crawler/kor_index_ohlcv/kor_index_ohlcv.csv', dtype = {'ticker': object})
# kor_index_list_df = pd.read_csv('data_crawler/kor_index_list_df/kor_index_list_df.csv')

conn = st.experimental_connection('gcs', type=FilesConnection)
kor_index_ohlcv = conn.read("finance-mlops-owen/data_crawler/kor_index_ohlcv/kor_index_ohlcv.csv", 
                      input_format="csv", ttl=600)
                      
kor_index_code_fundamental = conn.read("finance-mlops-owen/data_crawler/kor_index_code_fundamental/kor_index_code_fundamental.csv", 
                      input_format="csv", ttl=600)
                      
                      
kor_index_list_df = conn.read("finance-mlops-owen/data_crawler/kor_index_list_df/kor_index_list_df.csv", 
                      input_format="csv", ttl=600)


# kor_index_ohlcv = kor_index_ohlcv[kor_index_ohlcv['date'] > today_date2]
kor_index_ohlcv = kor_index_ohlcv[kor_index_ohlcv['date'] > '2023-07-15']

df = kor_index_ohlcv.groupby(['index_code'])['close'].apply(list).reset_index()


df2 = pd.merge(kor_index_list_df, df, 
        on = 'index_code', 
        how = 'left')
        
df_kospi = df2[df2['market'] == 'KOSPI']        
df_kosdaq = df2[df2['market'] == 'KOSDAQ']        


ohlcv_kospi = kor_index_ohlcv[kor_index_ohlcv['index_code'] == 1001]
price_change_percentage_kospi = kor_index_code_fundamental[kor_index_code_fundamental['index_code'] == 1001]



ohlcv_kospi_value = ohlcv_kospi['close'].tail(1).tolist()[0]
price_change_percentage_value = price_change_percentage_kospi['price_change_percentage'].tail(1).tolist()[0]


col1, col2 = st.columns(2)
col1.metric("코스피", ohlcv_kospi_value, price_change_percentage_value)


fig = go.Figure([go.Scatter(x=ohlcv_kospi['date'], y=ohlcv_kospi['close'])])

fig.update_layout(
    title = ' ',
#     title= f'{sig_area} 시군구별 {type_nm} 매매(실거래가)/전월세(보증금) 거래량',
    title_font_family="맑은고딕",
    title_font_size = 18,
    hoverlabel=dict(
#         bgcolor='white',
        bgcolor='black',
        font_size=15,
    ),
#     hovermode="x unified",
    hovermode="x",    
#     template='plotly_white', 
    template='plotly_dark',
    xaxis_tickangle=90,
    yaxis_tickformat = ',',
    legend = dict(orientation = 'h', xanchor = "center", x = 0.85, y= 1.1), 
    barmode='group'
)
    
fig.update_layout(
      xaxis=dict(
        showline=True,
        showgrid=False,
        showticklabels=True,
        linecolor='rgb(204, 204, 204)',
        linewidth=2,
        ticks='outside',
        tickfont=dict(
            family='Arial',
            size=12,
            color='rgb(82, 82, 82)',
        ),
    ),
    yaxis=dict(
        showgrid=False,
        zeroline=False,
        showline=False,
        showticklabels=False,
    ),
  autosize=False,
  height=80,
  margin=go.layout.Margin(
    
        # l=10, #left margin
        # r=10, #right margin
        # b=10, #bottom margin
        # t=50  #top margin

        l=0, #left margin
        r=0, #right margin
        b=0, #bottom margin
        t=0  #top margin
    ))
    
col1.plotly_chart(fig, use_container_width=True)



ohlcv_kospi = kor_index_ohlcv[kor_index_ohlcv['index_code'] == 2001]
price_change_percentage_kospi = kor_index_code_fundamental[kor_index_code_fundamental['index_code'] == 2001]



ohlcv_kospi_value = ohlcv_kospi['close'].tail(1).tolist()[0]
price_change_percentage_value = price_change_percentage_kospi['price_change_percentage'].tail(1).tolist()[0]




col2.metric("코스닥", ohlcv_kospi_value, price_change_percentage_value)

fig = go.Figure([go.Scatter(x=ohlcv_kospi['date'], y=ohlcv_kospi['close'])])

fig.update_layout(
    title = ' ',
#     title= f'{sig_area} 시군구별 {type_nm} 매매(실거래가)/전월세(보증금) 거래량',
    title_font_family="맑은고딕",
    title_font_size = 18,
    hoverlabel=dict(
#         bgcolor='white',
        bgcolor='black',
        font_size=15,
    ),
#     hovermode="x unified",
    hovermode="x",    
#     template='plotly_white', 
    template='plotly_dark',
    xaxis_tickangle=90,
    yaxis_tickformat = ',',
    legend = dict(orientation = 'h', xanchor = "center", x = 0.85, y= 1.1), 
    barmode='group'
)
    
fig.update_layout(
      xaxis=dict(
        showline=True,
        showgrid=False,
        showticklabels=True,
        linecolor='rgb(204, 204, 204)',
        linewidth=2,
        ticks='outside',
        tickfont=dict(
            family='Arial',
            size=12,
            color='rgb(82, 82, 82)',
        ),
    ),
    yaxis=dict(
        showgrid=False,
        zeroline=False,
        showline=False,
        showticklabels=False,
    ),
  autosize=False,
  height=80,
  margin=go.layout.Margin(
    
        # l=10, #left margin
        # r=10, #right margin
        # b=10, #bottom margin
        # t=50  #top margin

        l=0, #left margin
        r=0, #right margin
        b=0, #bottom margin
        t=0  #top margin
    ))
    
col2.plotly_chart(fig, use_container_width=True)


        
col1, col2 = st.columns(2)

col1.dataframe(
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

col2.dataframe(
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
