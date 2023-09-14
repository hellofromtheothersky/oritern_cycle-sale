from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import BaseOperator

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from airflow.utils.decorators import apply_defaults
from airflow.decorators import  task

from airflow.utils.task_group import TaskGroup

from airflow.utils.trigger_rule import TriggerRule

from includes.control_table import update_task_runtime, load_enable_task_config

from datetime import datetime, timedelta


class LoadLandingToStagingArea(BaseOperator):
    """
    load file from landing area to staging in general
    - with incremental: append data directly to the stage table
    - with full: need an intermediate step to find changed data (by comparing the two data), and then do like incremental way
    """
    def __init__(self, target_conn_id, task_config, *args, **kwargs):
        super().__init__(*args, **kwargs) # initialize the parent operator
        self.target_conn_id = target_conn_id
        self.task_config = task_config

    # execute() method that runs when a task uses this operator, make sure to include the 'context' kwarg.
    def execute(self, context):
        # write to Airflow task logs
        hook = MsSqlHook(mssql_conn_id=self.target_conn_id)
        hook.run("EXEC load_to_stage_table " + str(self.task_config['task_id']))


default_args = {
    'owner': 'hieu_nguyen',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}
def format_yyyymmdd(datetime):
    return datetime.strftime("%Y%m%d")


with DAG( 
    default_args=default_args,
    dag_id='staging',
    description='staging',
    start_date = datetime.now()- timedelta(days=2),
    schedule_interval='0 22 * * *',
    catchup=False,
    user_defined_macros={
        "format_yyyymmdd": format_yyyymmdd,  # Macro can be a variable/function
    },
) as dag:
    start = BashOperator(
        task_id='start',
        bash_command="echo START",
        do_xcom_push=False,
    )

    with TaskGroup(group_id='staging') as staging:
        landing_test_db_db = LoadLandingToStagingArea.partial(
            task_id='staging',
            target_conn_id='staging_staging_db_db',
            max_active_tis_per_dagrun=2,
        ).expand(task_config=load_enable_task_config('staging'))

    update_task_runtime = PythonOperator(
        task_id='update_task_runtime',
        python_callable=update_task_runtime,
        op_kwargs={'dag_id': 'staging','load_cf_task_id': 'load_enable_task_config'},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end = BashOperator(
        task_id='end',
        bash_command="echo END",
        do_xcom_push=False,
    )

    start>>staging>>update_task_runtime>>end