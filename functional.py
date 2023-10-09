import pandas as pd
import os
import geopandas as gpd
import glob
import plotly.express as px
import plotly.graph_objects as go
import json
from ta.trend import MACD 
from ta.momentum import StochasticOscillator 

import streamlit as st
from datetime import datetime

from st_files_connection import FilesConnection
from plotly.subplots import make_subplots

def func1(data):
  
    # MACD 
    macd = MACD(close=data['close'], 
                window_slow=26,
                window_fast=12, 
                window_sign=9)
    # Stochastic
    stoch = StochasticOscillator(high=data['high'],
                                 close=data['close'],
                                 low=data['low'],
                                 window=14, 
                                 smooth_window=3)
                                 
                             
    fig = make_subplots(rows=4, cols=1, shared_xaxes=True, vertical_spacing=0.01, row_heights=[0.5,0.1,0.2,0.2])
    
    fig.add_trace(go.Candlestick(
            x=data['date'],
            open=data['open'],
            high=data['high'],
            low=data['low'],
            close=data['close'],
            increasing_line_color= 'red', decreasing_line_color= 'blue')
    , row=1, col=1)
    
    
    fig.add_trace(go.Scatter(
        x=data['date'],
        y=data['MA5'],
        opacity=0.7,
        line=dict(color='blue', width=2),
        name='MA 5'))
    
    fig.add_trace(go.Scatter(
        x=data['date'],
        y=data['MA20'],
        opacity=0.7,
        line=dict(color='orange', width=2),
        name='MA 20'))
    
    fig.add_trace(go.Scatter(
        x=pd.concat([data['date'], data['date'][::-1]]),
        y=pd.concat([data['upper'], data['lower'][::-1]]),
        fill='toself',
    #     fillcolor='rgba(0,100,80,0.2)',
    #     line_color='rgba(255,255,255,0)',
        fillcolor='rgba(255,255,0,0.1)',
    #     line_color='rgba(255,255,255,0.2)',
        line=dict(color='rgba(255,255,255,0.2)', width=2),
        name='Bollinger Band',
    ))
    
    
    # fig.add_trace(go.Bar(x=data['date'], 
    #                      y=data['volume'],
    #                      name = 'volume'
    #                     ), row=2, col=1)
    
    
    # # Plot MACD trace on 3rd row
    # fig.add_trace(go.Bar(x=data['date'], 
    #                      y=macd.macd_diff()
    #                     ), row=3, col=1)
    fig.add_trace(go.Scatter(x=data['date'],
                             y=macd.macd(),
                             line=dict(color='white', width=2)
                            ), row=3, col=1)
    fig.add_trace(go.Scatter(x=data['date'],
                             y=macd.macd_signal(),
                             line=dict(color='blue', width=1)
                            ), row=3, col=1)
    
    # Plot stochastics trace on 4th row
    fig.add_trace(go.Scatter(x=data['date'],
                             y=stoch.stoch(),
                             line=dict(color='white', width=1)
                            ), row=4, col=1)
    fig.add_trace(go.Scatter(x=data['date'],
                             y=stoch.stoch_signal(),
                             line=dict(color='blue', width=1)
                            ), row=4, col=1)
    
    
    fig.update_layout(
        title = 'asdf',
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
        
    fig.update_layout(
        margin=go.layout.Margin(
            l=10, #left margin
            r=10, #right margin
            b=10, #bottom margin
            t=50  #top margin
        ),
        height=600, width=1000, 
        showlegend=False, 
        xaxis_rangeslider_visible=False
    )
    
    # fig.update_layout(height=600, width=1000, 
    #                   showlegend=False, 
    #                   xaxis_rangeslider_visible=False)
        
    
    
    # update y-axis label
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)
    fig.update_yaxes(title_text="MACD", showgrid=False, row=3, col=1)
    fig.update_yaxes(title_text="Stoch", row=4, col=1)
    
    # Plot volume trace on 2nd row
    colors = ['red' if row['open'] - row['close'] >= 0 
              else 'blue' for index, row in data.iterrows()]
    
    fig.add_trace(go.Bar(x=data['date'], 
                         y=data['volume'],
                         marker_color=colors,
                         name = 'volume'
                        ), row=2, col=1)
    
    # Plot MACD trace on 3rd row
    colors = ['blue' if val >= 0 
              else 'red' for val in macd.macd_diff()]
    fig.add_trace(go.Bar(x=data['date'], 
                         y=macd.macd_diff(),
                         marker_color=colors
                        ), row=3, col=1)
    
    # fig.update_layout(xaxis_rangeslider_visible=False)
    
    fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "sun"])], row = 1, col = 1)
    # fig.update_yaxes(showspikes=True)
    # fig.update_xaxes(
    # #     rangeslider_visible=True,
    #     rangeselector=dict(
    #         buttons=list([
    #             dict(count=1, label="1m", step="month", stepmode="backward"),
    #             dict(count=6, label="6m", step="month", stepmode="backward"),
    #             dict(count=1, label="YTD", step="year", stepmode="todate"),
    #             dict(count=1, label="1y", step="year", stepmode="backward"),
    # #             dict(step="all")
    #         ])
    #     )
    # )
    
    #fig.show()
    return fig
    
