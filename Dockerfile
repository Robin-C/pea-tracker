FROM python:3.8.12-slim

RUN pip install --upgrade pip
RUN pip install dbt apache-airflow virtualenv
WORKDIR /my_app