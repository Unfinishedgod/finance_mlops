FROM python:3.11

# 8080 포트 노출
EXPOSE 8080


WORKDIR ./

COPY . .

RUN pip3 install pip==23.3.1

RUN pip3 install -U finance-datareader

RUN pip3 install -r requirements.txt


CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app