import streamlit as st

st.set_page_config(
    page_title="Information",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)


with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)



st.image('./etc/architecture_main.png', caption='architecture')



markdown_1 = """
- [1-1. GCP Compute Engine 구축](https://unfinishedgod.netlify.app/2023/06/10/gcp-gcp/) 
- [1-2. Compute Engine 방화벽 및 MobaXterm 연결](https://unfinishedgod.netlify.app/2023/06/11/gcp-compute-engine-mobaxterm/) 
"""

markdown_2 = """
- [2-1. pykrx를 사용한 금융데이터 수집 PART 1](https://unfinishedgod.netlify.app/2023/07/03/python-pykrk-part-1/) 
- [2-2. pykrx를 사용한 금융데이터 수집 PART 2](https://unfinishedgod.netlify.app/2023/07/09/python-pykrk-part-2/) 
- [2-3. FinanceDataReader을 사용한 금융 데이터 수집 (S&P500, 비트코인)](https://unfinishedgod.netlify.app/2023/07/11/python-financedatareader-s-p500/) 
- [2-4. PublicDataReader 라이브러리를 사용한 FRED 데이터 수집](https://unfinishedgod.netlify.app/2023/07/26/python-publicdatareader-fred/) 
- [2-5. Chatgpt API 사용 및 네이버 뉴스 요약 응용](https://unfinishedgod.netlify.app/2023/07/14/python-chatgpt-api/) 
"""

markdown_3 = """
- [3-1. ubuntu에 postgresql 설치 및 vscode 연결](https://unfinishedgod.netlify.app/2023/06/13/postgresql-ubuntu-postgresql/) 
- [3-2. Cloud SQL DB 구축](https://unfinishedgod.netlify.app/2023/06/15/gcp-cloud-sql-db/) 
- [3-3. BigQuery, Storage - Python 연동](https://unfinishedgod.netlify.app/2023/06/20/gcp-bigquery-storage-python/) 
- [3-4 Cloud Storage를 통한 빅쿼리 테이블 생성](https://unfinishedgod.netlify.app/2023/05/20/cloud-storage/) 
"""

markdown_4 = """
- [4-1. Airflow 1. 설치](https://unfinishedgod.netlify.app/2023/07/18/airflow-airflow-1/) 
- [4-2. Airflow 2. 기본 세팅(예제 제거, PostgreSQL 연결)](https://unfinishedgod.netlify.app/2023/07/20/airflow-airflow-2-dag/) 
- [4-3. Airflow 3. Timezone 설정 및 DAG 테스트](https://unfinishedgod.netlify.app/2023/07/22/airflow-airflow-3-timezone-dag/) 
- [4-4. 주가 데이터 수집 파이프라인 1 (국내주식편)](https://unfinishedgod.netlify.app/2023/07/29/airflow-1/)
- [4-5. 주가 데이터 수집 파이프라인 2 (S&P 500 편)](https://unfinishedgod.netlify.app/2023/08/04/airflow-2-s-p500/)
"""

# markdown_5 = """
# 
# """


col1, col2, col3 = st.columns(3)

with col1:
  st.markdown(markdown_1)
with col2:
  st.markdown(markdown_2)  
with col3:  
  st.markdown(markdown_3)  
  

col4, col5, col6 = st.columns(3)

with col4:
  st.markdown(markdown_4)
with col5:
  st.markdown("markdown_5")
with col6:
  st.markdown("markdown_6")
