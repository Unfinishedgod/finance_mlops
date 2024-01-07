import pandas as pd
import os

import glob
import plotly.express as px
import plotly.graph_objects as go
import json

from ta.trend import MACD 
from ta.momentum import StochasticOscillator 

import streamlit as st
from datetime import datetime

import numpy as np

from st_files_connection import FilesConnection
from plotly.subplots import make_subplots




def macd_vis(df_raw, technical_indicator_nm):
    # fig = go.Figure()
    fig = make_subplots(rows=4, cols=1, shared_xaxes=True, vertical_spacing=0.01, row_heights=[0.5, 0.1, 0.2, 0.2])

    # 캔들스틱차트
    fig.add_trace(go.Candlestick(
        x=df_raw['date'],
        open=df_raw['open'],
        high=df_raw['high'],
        low=df_raw['low'],
        close=df_raw['close'],
        increasing_line_color= 'red', decreasing_line_color= 'blue', 
        name = ''), row = 1, col = 1)

    # MA 5 
    fig.add_trace(go.Scatter(x=df_raw['date'],y=df_raw['MA5'],
                             opacity=0.7,
                             line=dict(color='blue', width=2),
                             name='MA 5') , row = 1, col = 1)

    # MA 20
    fig.add_trace(go.Scatter(x=df_raw['date'],y=df_raw['MA20'],
                             opacity=0.7,
                             line=dict(color='orange', width=2),
                             name='MA 20'), row = 1, col = 1)

    # MA 60
    fig.add_trace(go.Scatter(x=df_raw['date'],y=df_raw['MA60'],
                             opacity=0.7,
                             line=dict(color='purple', width=2),
                             name='MA 60'), row = 1, col = 1)

    # MA 120
    fig.add_trace(go.Scatter(x=df_raw['date'],y=df_raw['MA120'],
                             opacity=0.7,
                             line=dict(color='green', width=2),
                             name='MA 120'), row = 1, col = 1)

    fig.add_trace(go.Scatter(
        x=pd.concat([df_raw['date'], df_raw['date'][::-1]]),
        y=pd.concat([df_raw['upper'], df_raw['lower'][::-1]]),
        fill='toself',
        fillcolor='rgba(255,255,0,0.1)',
        line=dict(color='rgba(255,255,255,0.2)', width=2),
        name='Bollinger Band',
        showlegend=False
    ), row = 1, col = 1)

    # for type_nm in type_list:
    #     index_list = df_raw_anal_date.index[df_raw_anal_date[technical_indicator_nm].str.contains(type_nm)]
    #     for index_nm in index_list:
    #         cross_date = df_raw['date'][index_nm]
    #         cross_value = df_raw['close'][index_nm]
    # 
    #         fig.add_annotation(x=cross_date, 
    #                            y=cross_value,
    #                            text=f'{type_nm} <br> {technical_indicator_nm}',
    #                            showarrow=True,
    #                            arrowhead=1,
    #                            row = 1, col = 1)                

    # Row 2 
    # volume
    colors = ['blue' if row['open'] - row['close'] >= 0 
              else 'red' for index, row in df_raw.iterrows()]

    fig.add_trace(go.Bar(x=df_raw['date'], 
                         y=df_raw['volume'],
                         marker_color=colors,
                         name = 'Volume',
                         showlegend=False
                        ), row=2, col=1)

    # MACD
    colors = ['red' if val >= 0 
              else 'blue' for val in df_raw['MACD_DIFF']]
    fig.add_trace(go.Bar(x=df_raw['date'], 
                         y=df_raw['MACD_DIFF'],
                         marker_color=colors,
                         name =  'MACD DIFF'
                        ), row=3, col=1)

    fig.add_trace(go.Scatter(x=df_raw['date'],
                             y=df_raw['MACD'],
                             line=dict(color='green', width=2),
                             name = 'MACD'
                            ), row=3, col=1)
    fig.add_trace(go.Scatter(x=df_raw['date'],
                             y=df_raw['MACD_Signal'],
                             line=dict(color='orange', width=1),
                             name = 'MACD Signal'
                            ), row=3, col=1)

    # Row 5
    # RSI
    fig.add_trace(go.Scatter(x=df_raw['date'],
                             y=df_raw['RSI'],
                             line=dict(color='red', width=1),
                             name = 'RSI'
                            ), row=4, col=1)
    
    # 수평 사각 영역 추가하기
    
    fig.add_hrect(y0=70, y1=100, line_width=0, fillcolor="white", opacity=0.1,
                  annotation_text="과매수구간", 
                  annotation_position="top right",
                  annotation_font_size=10,
                  annotation_font_color="red",
                  annotation_font_family="Times New Roman", row=4, col=1)
    
    fig.add_hrect(y0=0, y1=30, line_width=0, fillcolor="white", opacity=0.1,
                  annotation_text="과매도 구간", 
                  annotation_position="top right",
                  annotation_font_size=10,
                  annotation_font_color="blue",
                  annotation_font_family="Times New Roman", row=4, col=1)    
    
    
    # Rayout
    fig.update_layout(
        title = '삼성전자 주가',
        title_font_family="맑은고딕",
        title_font_size = 18,
        hoverlabel=dict(
            bgcolor='black',
            font_size=15,
        ),
        hovermode="x unified",
        template='plotly_dark',
        xaxis_tickangle=90,
        yaxis_tickformat = ',',
        legend = dict(orientation = 'h', xanchor = "center", x = 0.5, y= 1.2),
        barmode='group',
        margin=go.layout.Margin(
            l=10, #left margin
            r=10, #right margin
            b=10, #bottom margin
            t=100  #top margin
        ),
        height=600, width=900, 
    #     showlegend=False, 
        xaxis_rangeslider_visible=False
    )

    # update y-axis label
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)
    fig.update_yaxes(title_text="MACD", row=3, col=1)
    fig.update_yaxes(title_text="RSI", row=4, col=1)

    fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])
    return fig

