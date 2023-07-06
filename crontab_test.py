#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import datetime 
import os

# 현재 시간
now = datetime.datetime.now()
now_time = now.strftime('%Y-%m-%d %H:%M:%S')

# 데이터 프레임 생성
df = pd.DataFrame({
    'nowtime': now_time
}, index = [0])


file_name = 'cron_test'

# 파일 저장
if not os.path.exists(f'{file_name}.csv'):
    df.to_csv(f'{file_name}.csv', index=False, mode='w')
else:
    df.to_csv(f'{file_name}.csv', index=False, mode='a', header=False)

