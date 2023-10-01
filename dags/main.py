from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta


default_args = {
    'owner': 'hieu_nguyen',
    'retries': 0,
    'retry_delay': timedelta(minutes=0.5)
}


with DAG( 
    default_args=default_args,
    dag_id='oritern_ELT',
    description='control etl job including landing, staging ...',
    start_date = datetime.now()- timedelta(days=2),
    schedule_interval='0 22 * * *',
    catchup=False,
) as dag:
    start = BashOperator(
        task_id='start',
        bash_command="echo START",
    )

    landing = TriggerDagRunOperator(
        task_id="landing",
        trigger_dag_id="oritern_ELT_landing",
        wait_for_completion=True,
        deferrable=True,  # Note that this parameter only exists in Airflow 2.6+
    )

    staging = TriggerDagRunOperator(
        task_id="staging",
        trigger_dag_id="oritern_ELT_staging",
        wait_for_completion=True,
        deferrable=True,  # Note that this parameter only exists in Airflow 2.6+
    )

    load_dim = TriggerDagRunOperator(
        task_id="load_dim",
        trigger_dag_id="oritern_ELT_load_dim",
        wait_for_completion=True,
        deferrable=True,  # Note that this parameter only exists in Airflow 2.6+
    )

    load_fact = TriggerDagRunOperator(
        task_id="load_fact",
        trigger_dag_id="oritern_ELT_load_fact",
        wait_for_completion=True,
        deferrable=True,  # Note that this parameter only exists in Airflow 2.6+
    )

    end = BashOperator(
        task_id='end',
        bash_command="echo END",
    )

    start>>landing>>staging>>load_dim>>load_fact>>end
