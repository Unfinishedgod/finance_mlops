#!./bin/bash

python_dir= "/home/shjj08choi4/anaconda3/bin/python3"
file_dir="/home/shjj08choi4/finance_mlops/python_file"

# 현재 시간이 오후 6시 이후이면
if [ $(date +%H) -ge 18 -a $(date +%H) -lt 19 ]; then
    nohup ${python_dir} ${file_dir}/pykrx_crawler.py &
    nohup ${python_dir} ${file_dir}/pykrx_crawler_index.py &
fi
