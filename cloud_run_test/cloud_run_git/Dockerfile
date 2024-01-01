FROM python:3.10

EXPOSE 8080

WORKDIR ./

COPY . .

RUN pip3 install -r requirements.txt

CMD ["python3", "app.py"]
