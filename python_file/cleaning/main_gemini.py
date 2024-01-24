import os
import pandas as pd

python_dir="/home/shjj08choi4/anaconda3/bin/python3"
file_dir="/home/shjj08choi4/finance_mlops/python_file"


for i in range(4):
    os.system(f"nohup {python_dir} {file_dir}/cleaning/gemini_kospi.py {str(i)} >> gemini_kospi.txt &")
    os.system(f"nohup {python_dir} {file_dir}/cleaning/gemini_kosdaq.py {str(i)} >> gemini_kosdaq.txt &")

final_i = 4

os.system(f"nohup {python_dir} {file_dir}/cleaning/gemini_kospi.py {str(final_i)} >> gemini_kospi.txt &")
os.system(f"{python_dir} {file_dir}/cleaning/gemini_kosdaq.py {str(final_i)} >> gemini_kosdaq.txt")


os.system(f"{file_dir}/send_gmail.py 'gemini_코스피코스닥'")
