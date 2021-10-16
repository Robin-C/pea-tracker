#!/usr/bin/python3
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

# get paris's timezone to schedule the dag
local_tz = pendulum.timezone("Europe/Paris")

# let's setup arguments for our dag

my_dag_id = "dbt_snapshot"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'concurrency': 1,
    'start_date':datetime(2021, 10, 10, tzinfo=local_tz)
}

# dag declaration

dag = DAG(
    dag_id=my_dag_id,
    default_args=default_args,
    catchup=False,
    schedule_interval='*/30 * * * *' # every 30 minutes
)


bash_task1 = BashOperator(task_id='dbt_snapshot',
                         bash_command="cd /my_app/dbt && dbt snapshot",
                         dag=dag)

bash_task2 = BashOperator(task_id='dbt_test',
                         bash_command="cd /my_app/dbt && dbt test",
                         dag=dag)


bash_task1 >> bash_task2

