import os
import pandas as pd


for i in range(1, 5):
    i = str(i)
    os.system("python3 pykrx_crawler.py" + " " + i)
