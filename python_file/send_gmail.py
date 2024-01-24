import smtplib
from email.mime.text import MIMEText
import pandas as pd

import os
import sys

# 경로 변경
os.chdir('/home/shjj08choi4/finance_mlops')

df = pd.read_csv('key_value/gmail_key.csv')


result_nm = sys.argv[1]


# Gmail 계정 정보
username = "shjj08@gmail.com"
password = df['api_key'][0]

# 메일 정보
receiver = "shjj08@gmail.com"

# 메일 전송 정보 입력
server = smtplib.SMTP("smtp.gmail.com", 587)
server.starttls()
server.login(username, 
             password)


# Email 본문 내용
message = MIMEText(
f'''
pykrx 데이터 수집

{result_nm}

완료
'''
)

# Email 제목
message['Subject'] = f'pykrx 데이터 수집 {result_nm} 완료' 

mail_msg = message.as_string()


# 메일 전송 정보 입력
server.sendmail(username, 
                receiver,
                mail_msg)
server.quit()
