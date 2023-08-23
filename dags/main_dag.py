from airflow.operators.bash import BashOperator
from airflow import DAG

from datetime import datetime, timedelta


default_args = {
    'owner': 'hieu_nguyen',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}


with DAG( 
    default_args=default_args,
    dag_id='airflow_test',
    description='just test prepare for etl project',
    start_date = datetime.now()- timedelta(days=2),
    schedule_interval='* * * * *',
    catchup=False,
) as dag:
    start = BashOperator(
        task_id='start',
        bash_command="echo START",
    )

    end = BashOperator(
        task_id='end',
        bash_command="echo END",
    )

    start>>end
