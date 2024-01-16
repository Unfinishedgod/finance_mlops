import os
import pandas as pd

from datetime import datetime
from datetime import timedelta

timedelta_list = [8,7,6,5,4,1,0]
for time_delta_nm in timedelta_list:
    os.system(f"python3 pykrx_crawler.py {str(time_delta_nm)}")
