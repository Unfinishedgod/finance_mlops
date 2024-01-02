#!/usr/bin/env python
# coding: utf-8

import functions_framework
import pandas as pd
from datetime import timedelta
import FinanceDataReader as fdr

from time import sleep

import os
import time
from flask import Flask

app = Flask(__name__)

@app.route("/")
def hello_world():
    """Example Hello World route."""
    # name = os.environ.get("NAME", "World")
    
    strat_date = '2023-01-01'
    end_date = '2023-12-01'
    
    usdkrw = fdr.DataReader('USD/KRW', strat_date, end_date) # 달러 원화
    usdkrw = usdkrw.reset_index()
    print(len(usdkrw))
    
    usd_nm = len(usdkrw)
    
    return_msg = f"USD/KRW 의 {strat_date} {end_date} 데이터 개수는 {usd_nm}"

    return return_msg

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
