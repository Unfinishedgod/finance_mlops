#!/usr/bin/env python
# coding: utf-8

import functions_framework
import pandas as pd
from pykrx import stock
from pykrx import bond
from datetime import timedelta
import FinanceDataReader as fdr

from time import sleep

import os
import time


usdkrw = fdr.DataReader('USD/KRW', '2023-01-01', '2023-12-01') # 달러 원화
usdkrw = usdkrw.reset_index()
print(len(usdkrw))

@functions_framework.http
def hello_http(request):


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

