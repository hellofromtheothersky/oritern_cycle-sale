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

import pandas as pd

def load_enable_task_config(task_name):
    """
        return config parameter of every enable child task of a task from task_name
    """
    hook = MsSqlHook(mssql_conn_id='staging_db')
    config_df = hook.get_pandas_df(sql="EXEC load_enable_task_config {0}".format(task_name))
    return config_df.to_dict("records")


class CopyDatabaseToCsv(BaseOperator):
    """
    load data from table in database to csv file
    """

    def __init__(self, source_conn_id, *args, **kwargs):
        super().__init__(*args, **kwargs) # initialize the parent operator
        self.source_conn_id = source_conn_id

    # execute() method that runs when a task uses this operator, make sure to include the 'context' kwarg.
    def execute(self, context):
        # write to Airflow task logs
        tasks_config=load_enable_task_config(self.task_id)
        if len(tasks_config)!=0:
            hook = MsSqlHook(mssql_conn_id=self.source_conn_id)
            for task_config in tasks_config:
                table_name=task_config['source_schema']+'.'+task_config['source_table']
                csv_dir="{0}{1}.{2}".format(task_config['target_location'], task_config['target_table'], task_config['target_schema'])

                self.log.info('COPY TABLE -> FILE ({0}, {1})'.format(table_name, csv_dir))

                df = hook.get_pandas_df(sql="SELECT * from {0}".format(table_name))
                df.to_csv(csv_dir, index=False)
        # the return value of '.execute()' will be pushed to XCom by default


class CopyFile(BashOperator):
    # template_fields = ('bash_command', 'source_file', 'source_dir', 'target_file', 'target_dir')

    @apply_defaults #Looks for an argument named "default_args", and fills the unspecified arguments from it.
    def __init__(
            self,
            *args, **kwargs):

        tasks_config=load_enable_task_config(kwargs['task_id'])
        bash_str=""
        for task_config in tasks_config:
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

    landing_csv = CopyFile(
        task_id='landing_csv',
    )

    landing_excel = CopyFile(
        task_id='landing_excel',
    )

    landing_json = CopyFile(
        task_id='landing_json',
    )

    landing_bicycle_retailer_db = CopyDatabaseToCsv(
        task_id='landing_bicycle_retailer_db',
        source_conn_id='source_bicycle_retailer_db'
    )

    landing_hrdb_db = CopyDatabaseToCsv(
        task_id='landing_hrdb_db',
        source_conn_id='source_hrdb_db',
    )

    end = BashOperator(
        task_id='end',
        bash_command="echo END",
    )


    start>>[landing_csv, landing_excel, landing_json, landing_bicycle_retailer_db, landing_hrdb_db]>>end
