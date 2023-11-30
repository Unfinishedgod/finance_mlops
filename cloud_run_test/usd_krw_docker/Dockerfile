FROM python:3.11

WORKDIR ./

COPY . .

RUN pip3 install pip==23.3.1

RUN pip3 install -U finance-datareader

RUN pip3 install -r requirements.txt

CMD ["python3", "usd_krw.py"]

