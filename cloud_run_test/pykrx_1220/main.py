import functions_framework

#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import pandas_gbq
from pykrx import stock
from pykrx import bond
import FinanceDataReader as fdr
from datetime import timedelta

from time import sleep

import psycopg2 as pg2
from sqlalchemy import create_engine

from datetime import datetime
from datetime import timedelta

import os
import time

import glob
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage



@functions_framework.http
def hello_http(request):

    today_date1 = '20231207'
    file_name = 'pykrxtest_222'
    time_line = now1.strftime("%Y%m%d_%H:%M:%S")

    df_raw = stock.get_market_ohlcv('20231206',  market="ALL")
    print(len(df_raw))

    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'name' in request_json:
        name = request_json['name']
    elif request_args and 'name' in request_args:
        name = request_args['name']
    else:
        name = 'World'
    return 'Hello {}!'.format(name)

