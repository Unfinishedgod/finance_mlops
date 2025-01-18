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



conn = st.connection('gcs', type=FilesConnection)
                      
gemini_main = conn.read(f"finance-mlops-proj/data_crawler/dashboard/gemini_main_view.parquet",
                      input_format="parquet", ttl=3600)

gemini_main_max_date = gemini_main['date'].max()
gemini_main_max_date = '2024-03-04'
gemini_main = gemini_main[gemini_main['date'] == gemini_main_max_date]


with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)



# ### 날짜 설정
now = datetime.now()
now = now + timedelta(days=-30)

today_date2 = now.strftime('%Y-%m-%d')


today_date2 = '2024-03-04'


# kor_index_ohlcv = pd.read_csv('data_crawler/kor_index_ohlcv/kor_index_ohlcv.csv', dtype = {'ticker': object})
# kor_index_list_df = pd.read_csv('data_crawler/kor_index_list_df/kor_index_list_df.csv')

conn = st.connection('gcs', type=FilesConnection)
kor_index_ohlcv = conn.read("finance-mlops-proj/data_crawler/cleaning/kor_index_ohlcv/kor_index_ohlcv.parquet", 
                      input_format="parquet", ttl=600)
kor_index_code_fundamental = conn.read("finance-mlops-proj/data_crawler/kor_index_code_fundamental/kor_index_code_fundamental_20240206.csv", 
                      input_format="csv", ttl=600)
kor_index_list_df = conn.read("finance-mlops-proj/data_crawler/kor_index_list_df/kor_index_list_df.parquet", 
                      input_format="parquet", ttl=600)

# parquet
buy_sell_count_kospi = conn.read("finance-mlops-proj/data_crawler/cleaning/kor_stock_ohlcv/buy_sell_count_kospi.parquet",
                      input_format="parquet", ttl=600)


kor_index_ohlcv = kor_index_ohlcv[kor_index_ohlcv['date'] >= today_date2]

df = kor_index_ohlcv.groupby(['index_code'])['close'].apply(list).reset_index()
kor_index_list_df['index_code'] = kor_index_list_df['index_code'].astype(str)


df2 = pd.merge(kor_index_list_df, df,
        on = 'index_code',
        how = 'left')

df_kospi = df2[df2['market'] == 'KOSPI']
df_kosdaq = df2[df2['market'] == 'KOSDAQ']


ohlcv_kospi = kor_index_ohlcv[kor_index_ohlcv['index_code'] == '1001']
price_change_percentage_kospi = kor_index_code_fundamental[kor_index_code_fundamental['index_code'] == 1001]


ohlcv_kospi_value = ohlcv_kospi['close'].tail(1).tolist()[0]
price_change_percentage_value = price_change_percentage_kospi['price_change_percentage'].tail(1).tolist()[0]



try:
    message = gemini_main['response_msg'][0]
except:
    message = '증권보고서를 생성중입니다. 잠시만 기다려 주세요.'

# col1, col2, col3 = st.columns(3)
col1, col2= st.columns(2)

# with col1:
#     st.markdown("Gemini로 작성한 보고서 이며, 금융투자회사 직원 등 제3자에게 투자판단을 일임하는 경우에도 투자결과에 대한 책임은 결국 투자자 본인이 부담하게 된다는 점을 유념하시기 바랍니다.")
#     st.markdown(message)
with col1:
    st.metric("코스피", ohlcv_kospi_value, price_change_percentage_value)


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

    st.plotly_chart(fig, use_container_width=True)



    ohlcv_kospi = kor_index_ohlcv[kor_index_ohlcv['index_code'] == '2001']
    price_change_percentage_kospi = kor_index_code_fundamental[kor_index_code_fundamental['index_code'] == 2001]



    ohlcv_kospi_value = ohlcv_kospi['close'].tail(1).tolist()[0]
    price_change_percentage_value = price_change_percentage_kospi['price_change_percentage'].tail(1).tolist()[0]

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

    st.metric("코스닥", ohlcv_kospi_value, price_change_percentage_value)

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

    st.plotly_chart(fig, use_container_width=True)

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




st.sidebar.markdown(
  """
    - Email : shjj08@gmail.com
    - Blog : [unfinishedgod_netlify](https://unfinishedgod.netlify.app/)
"""
)
