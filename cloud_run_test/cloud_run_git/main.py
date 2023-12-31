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
    name = os.environ.get("NAME", "World")
    
    usdkrw = fdr.DataReader('USD/KRW', '2023-01-01', '2023-12-01') # 달러 원화
    usdkrw = usdkrw.reset_index()
    print(len(usdkrw))
    
    usd_nm = len(usdkrw)


    return f"Hello {name} {usd_nm}!"

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
