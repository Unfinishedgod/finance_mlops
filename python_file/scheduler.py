import os
import pandas as pd


py_dir = '/home/shjj08choi4/anaconda3/bin/python3'

pykrx_dir = '/home/shjj08choi4/finance_mlops/python_file/total_crawler.py'
cleaning_dir = '/home/shjj08choi4/finance_mlops/python_file/cleaning.py'

# pykrx
print('pykrx start')
os.system(f'{py_dir} {pykrx_dir}')
print('pykrx end')


# cleaning 
print('cleaning start')
os.system(f'{py_dir} {cleaning_dir}')
print('cleaning end')
