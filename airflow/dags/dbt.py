#!/usr/bin/python3
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

# get paris's timezone to schedule the dag
local_tz = pendulum.timezone("Europe/Paris")

# let's setup arguments for our dag

my_dag_id = "dbt"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'concurrency': 2,
    'start_date':datetime(2021, 10, 10, tzinfo=local_tz)
}

# dag declaration

dag = DAG(
    dag_id=my_dag_id,
    default_args=default_args,
    catchup=False,
    schedule_interval= '0 0 * * *' # every day at midnight
)


# Here's a task based on Bash Operator!

bash_task1 = BashOperator(task_id='tickers',
                         bash_command=". /my_app/extract_load_scripts/env_extract_load_scripts/bin/activate && python /my_app/extract_load_scripts/get_tickers/get_tickers.py && deactivate",
                         dag=dag)

bash_task2 = BashOperator(task_id='coins',
                         bash_command=". /my_app/extract_load_scripts/env_extract_load_scripts/bin/activate && python /my_app/extract_load_scripts/get_crypto_coins/get_coins.py && deactivate",
                         dag=dag)

bash_task3 = BashOperator(task_id='dbt_snapshot',
                         bash_command="cd /my_app/dbt && dbt snapshot",
                         dag=dag)

bash_task4 = BashOperator(task_id='prices',
                         bash_command=". /my_app/extract_load_scripts/env_extract_load_scripts/bin/activate && python /my_app/extract_load_scripts/get_prices/get_prices.py && deactivate",
                         dag=dag)

bash_task5 = BashOperator(task_id='crypto_prices',
                         bash_command=". /my_app/extract_load_scripts/env_extract_load_scripts/bin/activate && cd /my_app/extract_load_scripts/get_crypto_prices/  && scrapy crawl coingecko&& deactivate",
                         dag=dag)

bash_task6 = BashOperator(task_id='transactions',
                         bash_command=". /my_app/extract_load_scripts/env_extract_load_scripts/bin/activate && python /my_app/extract_load_scripts/get_transactions/get_transactions.py && deactivate",
                         dag=dag)

bash_task7 = BashOperator(task_id='crypto_transactions',
                         bash_command=". /my_app/extract_load_scripts/env_extract_load_scripts/bin/activate && python /my_app/extract_load_scripts/get_crypto_transactions/get_crypto_transactions.py && deactivate",
                         dag=dag)

bash_task8 = BashOperator(task_id='dbt_run',
                         bash_command="cd /my_app/dbt && dbt run",
                         dag=dag)

bash_task9 = BashOperator(task_id='dbt_test',
                         bash_command="cd /my_app/dbt && dbt test",
                         dag=dag)


[bash_task1, bash_task2] >> bash_task3 >> [bash_task4, bash_task5, bash_task6, bash_task7]  >> bash_task8 >> bash_task9