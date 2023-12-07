FROM python:3.10

EXPOSE 8080

WORKDIR ./

COPY . .

RUN pip3 install -r requirements.txt

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app