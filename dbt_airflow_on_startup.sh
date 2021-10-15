#!/bin/bash

# install the packages to run the python scripts
cd /my_app/extract_load_scripts 
virtualenv env_extract_load_scripts
. env_extract_load_scripts/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
deactivate

# install ps
apt-get update && apt-get install -y procps

airflow db init

# create user for airflow web ui
airflow users create -u admin -p admin -r Admin -e email@email.com -f Robin -l CHESNE

# start webserver
airflow webserver &

# and the scheduler
airflow scheduler &

# build docs & serve it on port 8001
cd /my_app/dbt && dbt docs generate && dbt docs serve --port 8001