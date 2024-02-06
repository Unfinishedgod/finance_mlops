#!/usr/bin/env python
# coding: utf-8

import os


python_dir="/home/shjj08choi4/anaconda3/bin/python3"
file_dir="/home/shjj08choi4/finance_mlops/python_file"

for i in range(5):
    os.system(f"nohup {python_dir} {file_dir}/cleaning/gemini_kospi.py {str(i)} >> gemini_kospi.txt &")
    os.system(f"nohup {python_dir} {file_dir}/cleaning/gemini_kosdaq.py {str(i)} >> gemini_kosdaq.txt &")

os.system(f"/home/shjj08choi4/anaconda3/bin/python3 python_file/send_gmail.py 'gemini_코스피코스닥'")
