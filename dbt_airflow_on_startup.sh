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

# create user for airflow web ui
airflow db init
airflow users create -u admin -p admin -r Admin -e email@email.com -f Robin -l CHESNE

# start webserver
airflow webserver &

# and the scheduler
airflow scheduler