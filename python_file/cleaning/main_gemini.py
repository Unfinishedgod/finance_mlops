import os
import pandas as pd

python_dir="/home/shjj08choi4/anaconda3/bin/python3"
file_dir="/home/shjj08choi4/finance_mlops/python_file"


for i in range(5):
    os.system(f"nohup {python_dir} {file_dir}/cleaning/gemini_kospi.py {str(i)} >> gemini_kospi_{i}.txt &")
    os.system(f"nohup {python_dir} {file_dir}/cleaning/gemini_kosdaq.py {str(i)} >> gemini_kosdaq_{i}.txt &")