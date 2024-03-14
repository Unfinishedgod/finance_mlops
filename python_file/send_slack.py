import slack_sdk

import pandas as pd
import os
import sys

# 경로 변경
os.chdir('/home/shjj08choi4/finance_mlops')

df = pd.read_csv('key_value/slack_key.csv')

result_nm = sys.argv[1]

slack_token = df['api_key'][0]

client = slack_sdk.WebClient(token=slack_token)

# 멘션할 사용자 ID
user_id = "UEFAUKXED"

slack_msg = f'<@{user_id}> pykrx 데이터 수집 {result_nm} 완료' 


# 메시지 전송
response = client.chat_postMessage(
    channel="slack_msg",
    text=slack_msg
)

# slack_msg = f'<@owen> pykrx 데이터 수집 {result_nm} 완료' 

# client.chat_postMessage(channel = 'slack_msg', text = slack_msg)