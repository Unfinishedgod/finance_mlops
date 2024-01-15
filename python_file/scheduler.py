import os
import pandas as pd
from datetime import datetime


py_dir = '/home/shjj08choi4/anaconda3/bin/python3'

file_dir = '/home/shjj08choi4/finance_mlops/python_file'

############################################################################
############################################################################

# pykrx  
time_line = datetime.now().strftime("%Y%m%d_%H:%M:%S")
print(f'pykrx 시작 {time_line}')

os.system(f'{py_dir} {file_dir}/pykrx_crawler.py')

time_line = datetime.now().strftime("%Y%m%d_%H:%M:%S")
print(f'pykrx 완료 {time_line}')

############################################################################
############################################################################

# pykrx index crawler
time_line = datetime.now().strftime("%Y%m%d_%H:%M:%S")
print(f'pykrx index 시작 {time_line}')

os.system(f'{py_dir} {file_dir}/pykrx_crawler_index.py')

time_line = datetime.now().strftime("%Y%m%d_%H:%M:%S")
print(f'pykrx index 완료 {time_line}')

############################################################################
############################################################################

# pykrx cleaning kospi
time_line = datetime.now().strftime("%Y%m%d_%H:%M:%S")
print(f'pykrx cleaning kospi 시작 {time_line}')

os.system(f'{py_dir} {file_dir}/cleaning/cleaning_stock_kospi.py')

time_line = datetime.now().strftime("%Y%m%d_%H:%M:%S")
print(f'pykrx cleaning kospi 완료 {time_line}')

############################################################################
############################################################################

# pykrx cleaning kosdaq
time_line = datetime.now().strftime("%Y%m%d_%H:%M:%S")
print(f'pykrx cleaning kosdaq 시작 {time_line}')

os.system(f'{py_dir} {file_dir}/cleaning/cleaning_stock_kosdaq.py')

time_line = datetime.now().strftime("%Y%m%d_%H:%M:%S")
print(f'pykrx cleaning kosdaq 완료 {time_line}')

############################################################################
############################################################################

# pykrx cleaning index
time_line = datetime.now().strftime("%Y%m%d_%H:%M:%S")
print(f'pykrx cleaning index 시작 {time_line}')

os.system(f'{py_dir} {file_dir}/cleaning/cleaning_index.py')

time_line = datetime.now().strftime("%Y%m%d_%H:%M:%S")
print(f'pykrx cleaning index 완료 {time_line}')

############################################################################
############################################################################


# pykrx indicator_setup
time_line = datetime.now().strftime("%Y%m%d_%H:%M:%S")
print(f'pykrx indicator_setup 시작 {time_line}')

os.system(f'{py_dir} {file_dir}/cleaning/indicator_setup.py')

time_line = datetime.now().strftime("%Y%m%d_%H:%M:%S")
print(f'pykrx indicator_setup 완료 {time_line}')

############################################################################
############################################################################



