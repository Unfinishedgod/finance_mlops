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

st.set_page_config(
    page_title="Bitcoin",
    page_icon="🪙",
    layout="wide",
    initial_sidebar_state="expanded"
)


with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)




conn = st.experimental_connection('gcs', type=FilesConnection)
bitcoin_df = conn.read("finance-mlops-owen/data_crawler/bitcoin/bitcoin_20230908.csv", 
                      input_format="csv", ttl=600)



# bitcoin_df = pd.read_csv('data_crawler/bitcoin/bitcoin_20230908.csv')


option = 'Bitcoin'

bitcoin_df['MA120'] = bitcoin_df['close'].rolling(window=120).mean()
bitcoin_df['MA60'] = bitcoin_df['close'].rolling(window=60).mean()
bitcoin_df['MA20'] = bitcoin_df['close'].rolling(window=20).mean()
bitcoin_df['MA5'] = bitcoin_df['close'].rolling(window=5).mean()


# bitcoin_df = bitcoin_df[bitcoin_df['date'] > '2022-08-01']

# tab1, tab2 = st.tabs(["📈 Chart", "🗃 Data"])

# fig = make_subplots(rows=4, cols=1, shared_xaxes=True)
# fig = make_subplots(rows=4, cols=1, shared_xaxes=True, vertical_spacing=0.01, row_heights=[0.5,0.1,0.2,0.2])
fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.01, row_heights=[0.7,0.3])
# Plot OHLC on 1st subplot (using the codes from before)
# Plot volume trace on 2nd row
fig.add_trace(go.Candlestick(
        x=bitcoin_df['date'],
        open=bitcoin_df['open'],
        high=bitcoin_df['high'],
        low=bitcoin_df['low'],
        close=bitcoin_df['close'],
        increasing_line_color= 'red', decreasing_line_color= 'blue')
, row=1, col=1)

fig.add_trace(go.Scatter(x=bitcoin_df['date'],
                         y=bitcoin_df['MA5'],
                         opacity=0.7,
                         line=dict(color='blue', width=2),
                         name='MA 5'))
fig.add_trace(go.Scatter(x=bitcoin_df['date'],
                         y=bitcoin_df['MA20'],
                         opacity=0.7,
                         line=dict(color='orange', width=2),
                         name='MA 20'))
fig.add_trace(go.Scatter(x=bitcoin_df['date'],
                         y=bitcoin_df['MA60'],
                         opacity=0.7,
                         line=dict(color='green', width=2),
                         name='MA 60'))
fig.add_trace(go.Scatter(x=bitcoin_df['date'],
                         y=bitcoin_df['MA120'],
                         opacity=0.7,
                         line=dict(color='yellow', width=2),
                         name='MA 120'))                         
                         

# fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])

# fig.add_trace(go.Scatter(x=bitcoin_df['date'], y=bitcoin_df['close']), row=2, col=1)



fig.add_trace(go.Bar(x=bitcoin_df['date'], 
                     y=bitcoin_df['volume'],
                     name = 'volumn'
                    ), row=2, col=1)



fig.update_layout(
    title = option,
#     title= f'{sig_area} 시군구별 {type_nm} 매매(실거래가)/전월세(보증금) 거래량',
    title_font_family="맑은고딕",
    title_font_size = 18,
    hoverlabel=dict(
#         bgcolor='white',
        bgcolor='black',
        font_size=15,
    ),
    hovermode="x unified",
#     hovermode="x",    
#     template='plotly_white', 
    template='plotly_dark',
    xaxis_tickangle=90,
    yaxis_tickformat = ',',
    legend = dict(orientation = 'h', xanchor = "center", x = 0.85, y= 1.1), 
    barmode='group'
)
    
# fig.update_layout(margin=go.layout.Margin(
#         l=20, #left margin
#         r=20, #right margin
#         b=20, #bottom margin
#         t=20  #top margin
#     ))
fig.update_layout(xaxis_rangeslider_visible=False)



# fig = go.Figure(
#     data=go.Candlestick(
#         x=bitcoin_df['date'],
#         open=bitcoin_df['open'],
#         high=bitcoin_df['high'],
#         low=bitcoin_df['low'],
#         close=bitcoin_df['close'],
#         increasing_line_color= 'red', decreasing_line_color= 'blue')
# )
# 
# 
# fig.add_trace(go.Scatter(x=bitcoin_df['date'],
#                          y=bitcoin_df['MA5'],
#                          opacity=0.7,
#                          line=dict(color='blue', width=2),
#                          name='MA 5'))
# fig.add_trace(go.Scatter(x=bitcoin_df['date'],
#                          y=bitcoin_df['MA20'],
#                          opacity=0.7,
#                          line=dict(color='orange', width=2),
#                          name='MA 20'))
# 
# 
# fig.update_layout(
#     title = option,
# #     title= f'{sig_area} 시군구별 {type_nm} 매매(실거래가)/전월세(보증금) 거래량',
#     title_font_family="맑은고딕",
#     title_font_size = 18,
#     hoverlabel=dict(
# #         bgcolor='white',
#         bgcolor='black',
#         font_size=15,
#     ),
#     hovermode="x unified",
# #     hovermode="x",
# #     template='plotly_white',
#     template='plotly_dark',
#     xaxis_tickangle=90,
#     yaxis_tickformat = ',',
#     legend = dict(orientation = 'h', xanchor = "center", x = 0.85, y= 1.1),
#     barmode='group'
# )
# 
# fig.update_layout(margin=go.layout.Margin(
#         l=10, #left margin
#         r=10, #right margin
#         b=10, #bottom margin
#         t=50  #top margin
#     ))
# 
# # fig.update_layout(xaxis_rangeslider_visible=False)
# fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])
# # fig.show()

st.plotly_chart(fig, use_container_width=True)
