FROM python:3.10-slim

RUN apt-get update && apt-get install -y curl
RUN pip install mlflow==2.20.1 psycopg2-binary boto3 cryptography pymysql

EXPOSE 5000
