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


class LoadStgToFact(BaseOperator):
    def __init__(self, wh_conn_id, task_config, *args, **kwargs):
        super().__init__(*args, **kwargs) # initialize the parent operator
        self.wh_conn_id = wh_conn_id
        self.task_config = task_config

    # execute() method that runs when a task uses this operator, make sure to include the 'context' kwarg.
    def execute(self, context):
        # write to Airflow task logs
        hook = MsSqlHook(mssql_conn_id=self.wh_conn_id)
        hook.run("EXEC load_to_fact " + str(self.task_config['task_id']))


default_args = {
    'owner': 'hieu_nguyen',
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}
def format_yyyymmdd(datetime):
    return datetime.strftime("%Y%m%d")


with DAG( 
    default_args=default_args,
    dag_id='load_fact',
    description='load_fact',
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

    with TaskGroup(group_id='load_fact_gr') as load_fact_gr:
        landing_test_db_db = LoadStgToFact.partial(
            task_id='load_fact',
            wh_conn_id='warehouse_warehouse_db_db',
            max_active_tis_per_dagrun=2,
        ).expand(task_config=load_enable_task_config('load_fact'))

    update_task_runtime = PythonOperator(
        task_id='update_task_runtime',
        python_callable=update_task_runtime,
        op_kwargs={'dag_id': 'load_fact','load_cf_task_id': 'load_fact_gr.load_enable_task_config'},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end = BashOperator(
        task_id='end',
        bash_command="echo END",
        do_xcom_push=False,
    )

    start>>load_fact_gr>>update_task_runtime>>end