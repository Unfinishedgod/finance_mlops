#!./bin/bash

python_dir="/home/shjj08choi4/anaconda3/bin/python3"
file_dir="/home/shjj08choi4/finance_mlops/python_file"

# 현재 시간이 오후 6시 이후이면 오후 7시 이전이면
if [ $(date +%H) -ge 18 -a $(date +%H) -lt 19 ]; then

    echo "수집: pykrx 수집 시작"
    ${python_dir} ${file_dir}/pykrx_crawler.py

    echo "수집: pykrx index 수집 시작"
    ${python_dir} ${file_dir}/pykrx_crawler_index.py

    echo "전처리: pykrx cleaning 코스피 시작"
    ${python_dir} ${file_dir}/cleaning/cleaning_stock_kospi.py

    echo "전처리: pykrx cleaning 코스닥 시작"
    ${python_dir} ${file_dir}/cleaning/cleaning_stock_kosdaq.py

    echo "전처리: pykrx cleaning index 시작"
    ${python_dir} ${file_dir}/cleaning/cleaning_index.py

    echo "보조지표: pykrx 보조지표 시작"
    ${python_dir} ${file_dir}/cleaning/indicator_setup.py

    echo "Gemini: 메인 시작"
    ${python_dir} ${file_dir}/cleaning/gemini_main_view.py

    echo "Gemini: 코스피 & 코스닥 시작"
    ${python_dir} ${file_dir}/cleaning/main_gemini.py

fi
