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


class LoadStgToDim(BaseOperator):
    def __init__(self, wh_conn_id, task_config, *args, **kwargs):
        super().__init__(*args, **kwargs) # initialize the parent operator
        self.wh_conn_id = wh_conn_id
        self.task_config = task_config

    # execute() method that runs when a task uses this operator, make sure to include the 'context' kwarg.
    def execute(self, context):
        # write to Airflow task logs
        hook = MsSqlHook(mssql_conn_id=self.wh_conn_id)
        hook.run("EXEC load_to_dim_scd2 " + str(self.task_config['task_id']))


default_args = {
    'owner': 'hieu_nguyen',
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}
def format_yyyymmdd(datetime):
    return datetime.strftime("%Y%m%d")


with DAG( 
    default_args=default_args,
    dag_id='oritern_ELT_load_dim',
    description='load_dim',
    start_date = datetime.now()- timedelta(days=2),
    schedule=None,
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

    with TaskGroup(group_id='load_dim_gr') as load_dim_gr:
        landing_test_db_db = LoadStgToDim.partial(
            task_id='load_dim',
            wh_conn_id='warehouse_warehouse_db_db',
            max_active_tis_per_dagrun=2,
        ).expand(task_config=load_enable_task_config('load_dim'))

    update_task_runtime = PythonOperator(
        task_id='update_task_runtime',
        python_callable=update_task_runtime,
        op_kwargs={'dag_id': 'oritern_ELT_load_dim','load_cf_task_id': 'load_dim_gr.load_enable_task_config'},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end = BashOperator(
        task_id='end',
        bash_command="echo END",
        do_xcom_push=False,
    )

    start>>load_dim_gr>>update_task_runtime>>end