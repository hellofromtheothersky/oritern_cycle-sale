from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.hooks.base_hook import BaseHook

from airflow.models.connection import Connection

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.models.dagrun import DagRun
from airflow.models.dagbag import DagBag

from airflow.utils.task_group import TaskGroup


from airflow.decorators import dag, task

import pandas as pd

@task
def load_enable_task_config(task_name):
    """
        return config parameter of every enable child task of a task from task_name
    """
    hook = MsSqlHook(mssql_conn_id='staging_db')
    config_df = hook.get_pandas_df(sql="EXEC load_enable_task_config {0}".format(task_name))
    return config_df.to_dict("records")



def update_task_runtime():
    pass
    # dag = DagBag().get_dag('landing')
    # last_dagrun_run_id = dag.get_last_dagrun(include_externally_triggered=True)

    # dag_runs = DagRun.find(dag_id='landing')
    # for dag_run in dag_runs:
    # # get the dag_run details for the Dag that triggered this
    #     if dag_run.execution_date == last_dagrun_run_id.execution_date:
    #         # get the DAG-level dag_run metadata!
    #         dag_run_tasks = dag_run.get_task_instances()
    #         for task in dag_run_tasks:
    #             # get the TASK-level dag_run metadata!
    #             hook = MsSqlHook(mssql_conn_id='staging_db')
    #             hook.run("EXEC set_task_config")


class CopyTableToCsv(BaseOperator):
    """
    load data from table in database to csv file
    """

    def __init__(self, source_conn_id, task_config, *args, **kwargs):
        super().__init__(*args, **kwargs) # initialize the parent operator
        self.source_conn_id = source_conn_id
        self.task_config = task_config

    # execute() method that runs when a task uses this operator, make sure to include the 'context' kwarg.
    def execute(self, context):
        # write to Airflow task logs
        hook = MsSqlHook(mssql_conn_id=self.source_conn_id)
        table_name=self.task_config['source_schema']+'.'+self.task_config['source_table']
        csv_dir="{0}{1}.{2}".format(self.task_config['target_location'], self.task_config['target_table'], self.task_config['target_schema'])

        self.log.info('COPY TABLE -> FILE ({0}, {1})'.format(table_name, csv_dir))

        df = hook.get_pandas_df(sql="SELECT * from {0}".format(table_name))
        df.to_csv(csv_dir, index=False)
        # the return value of '.execute()' will be pushed to XCom by default


class CopyFile(BashOperator):
    # template_fields = ('bash_command', 'source_file', 'source_dir', 'target_file', 'target_dir')

    @apply_defaults #Looks for an argument named "default_args", and fills the unspecified arguments from it.
    def __init__(
            self,
            task_config,
            *args, **kwargs):

        bash_str=""
        source_dir=task_config['source_location']
        target_dir="{0}{1}.{2}".format(task_config['target_location'], task_config['target_table'], task_config['target_schema'])

        self.log.info('COPY FILE -> FILE ({0}, {1})'.format(source_dir, target_dir)) #not run
        bash_str+="cp {0} {1};".format(source_dir, target_dir)
            
        super(CopyFile, self).__init__(bash_command=bash_str, *args, **kwargs)


default_args = {
    'owner': 'hieu_nguyen',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

# def add_function(x: int):
#     print(x)
#     return x


# @task
# def minus(x: int, y: int):
#     return x + y


with DAG( 
    default_args=default_args,
    dag_id='landing',
    description='landing',
    start_date = datetime.now()- timedelta(days=2),
    schedule_interval='0 22 * * *',
    catchup=False,
) as dag:
    start = BashOperator(
        task_id='start',
        bash_command="echo START",
    )

    with TaskGroup(group_id='landing') as landing:
        landing_bicycle_retailer_db = CopyTableToCsv.partial(
            task_id='landing_bicycle_retailer_db',
            source_conn_id='source_bicycle_retailer_db'
        ).expand(task_config=load_enable_task_config('landing_bicycle_retailer_db'))

        landing_hrdb_db = CopyTableToCsv.partial(
            task_id='landing_hrdb_db',
            source_conn_id='source_hrdb_db'
        ).expand(task_config=load_enable_task_config('landing_hrdb_db'))

        landing_csv = CopyFile.partial(
            task_id='landing_csv',
        ).expand(task_config=load_enable_task_config('landing_csv'))

        landing_excel = CopyFile.partial(
            task_id='landing_excel',
        ).expand(task_config=load_enable_task_config('landing_excel'))

        landing_json = CopyFile.partial(
            task_id='landing_json',
        ).expand(task_config=load_enable_task_config('landing_json'))

    # update_task_runtime = PythonOperator(
    #     task_id='update_task_runtime',
    #     python_callable=update_task_runtime
    # )

    end = BashOperator(
        task_id='end',
        bash_command="echo END",
    )

    # added_values = PythonOperator.partial(
    #     task_id="added_values",
    #     python_callable=add_function,
    # ).expand(op_args=[[1], [2], [3]])

    # minused_values = minus.partial(y=10).expand(x=[1, 2, 3])

    # start>>[landing_csv, landing_json, landing_bicycle_retailer_db, landing_hrdb_db]>>end
    # start >> update_task_runtime >> landing_excel >> end
    start>>landing>>end