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

from airflow.utils.trigger_rule import TriggerRule

from airflow.exceptions import AirflowException

import pandas as pd
import json
import re
from dateutil import parser
from datetime import datetime
import os

@task
def load_enable_task_config(task_name, ti=None):
    """
        return config parameter of every enable child task of a task from task_name
    """
    hook = MsSqlHook(mssql_conn_id='staging_db')
    config_df = hook.get_pandas_df(sql="EXEC load_enable_task_config {0}".format(task_name))
    config_df_dict=config_df.to_dict("records")
    
    mapped_index={}

    for i in range(len(config_df_dict)):
        if task_name not in mapped_index.keys():
            mapped_index[task_name]={}
        mapped_index[task_name][int(i)]=config_df_dict[i]['task_id']

    ti.xcom_push(key="mapped_index", value=mapped_index)

    return config_df_dict


def update_task_runtime(ti):
    mapped_index={}
    i=0
    while i>=0:
        sub="__"+str(i)
        if i==0:
            sub=""
        a=ti.xcom_pull(task_ids="landing.load_enable_task_config"+sub, key="mapped_index")
        if a:
            mapped_index.update(a)
            i+=1
        else: i=-1

    hook = MsSqlHook(mssql_conn_id='staging_db')
    dag = DagBag().get_dag('landing')
    last_dagrun_run_id = dag.get_last_dagrun(include_externally_triggered=True)

    dag_runs = DagRun.find(dag_id='landing')
    for dag_run in dag_runs:
    # get the dag_run details for the Dag that triggered this
        if dag_run.execution_date == last_dagrun_run_id.execution_date:
            # get the DAG-level dag_run metadata!
            dag_run_tasks = dag_run.get_task_instances()
            for task in dag_run_tasks:
                # print(task.task_id, task.map_index, task.start_date, task.end_date, task.duration, task.state)
                try:
                    task_id=task.task_id.split('.')[1]
                except IndexError:
                    pass
                else:
                    if task_id in mapped_index.keys():
                        task_id_in_cf_table=mapped_index[task_id][str(task.map_index)]
                        # get the TASK-level dag_run metadata!
                        hook.run("EXEC set_task_config {0}, '{1}', '{2}', {3}, '{4}'".format(task_id_in_cf_table, task.start_date, task.end_date, task.duration, task.state))


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

        if not os.path.exists(self.task_config['target_location']):
            os.makedirs(self.task_config['target_location'])

        # self.log.info('COPY TABLE -> FILE ({0}, {1})'.format(table_name, csv_dir))

        df = hook.get_pandas_df(sql="SELECT * from {0}".format(table_name))
        df.to_csv(csv_dir, mode='w', index=False)


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

        # self.log.info('COPY FILE -> FILE ({0}, {1})'.format(source_dir, target_dir)) #not run
        bash_str+="mkdir -p {0}; cp {1} {2};".format(task_config['target_location'], source_dir, target_dir)
            
        super(CopyFile, self).__init__(bash_command=bash_str, *args, **kwargs)


class Archiving(BashOperator):
    # template_fields = ('bash_command', 'source_file', 'source_dir', 'target_file', 'target_dir')

    @apply_defaults #Looks for an argument named "default_args", and fills the unspecified arguments from it.
    def __init__(
            self,
            # logical_date,
            *args, **kwargs):

        bash_str="""
                source_folder="/opt/airflow/landing_cycle-sale/"
                
                for source_sub_folder in $source_folder/*/; do
                    folder_name=$(basename $source_sub_folder)
                    if [[ $folder_name == '*' || $folder_name == 'archive' ]]; then
                        continue
                    fi
                    destination_folder=${source_folder}archive/$folder_name
                    mkdir -p $destination_folder

                    for source_file in $source_sub_folder/*; do
                        if [[ -f $source_file ]]; then
                            filename=$(basename $source_file)
                            extension=${filename##*.}
                            filename_no_ext=${filename%.*}
                            new_filename=${filename_no_ext}_"""+"{{ format_yyyymmdd(data_interval_start) }}"+""".${extension}

                            mv $source_file ${destination_folder}/${new_filename}
                        fi
                    done
                done
                """
            
        super(Archiving, self).__init__(bash_command=bash_str, *args, **kwargs)


default_args = {
    'owner': 'hieu_nguyen',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

# @task(trigger_rule=TriggerRule.ALL_FAILED, retries=0)
# def watcher():
#     raise AirflowException("Failing task because one or more upstream tasks failed.")
def format_yyyymmdd(datetime):
    return datetime.strftime("%Y%m%d")


with DAG( 
    default_args=default_args,
    dag_id='landing',
    description='landing',
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

    archiving = Archiving(
        task_id='archiving',
        # logical_date="{{ ti.xcom_pull(task_ids='start') }}"
    )


    with TaskGroup(group_id='landing') as landing:
        landing_bicycle_retailer_db = CopyTableToCsv.partial(
            task_id='landing_bicycle_retailer_db',
            source_conn_id='source_bicycle_retailer_db',
        ).expand(task_config=load_enable_task_config('landing_bicycle_retailer_db'))

        landing_hrdb_db = CopyTableToCsv.partial(
            task_id='landing_hrdb_db',
            source_conn_id='source_hrdb_db',
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

    update_task_runtime = PythonOperator(
        task_id='update_task_runtime',
        python_callable=update_task_runtime,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end = BashOperator(
        task_id='end',
        bash_command="echo END",
        do_xcom_push=False,
    )

    # added_values = PythonOperator.partial(
    #     task_id="added_values",
    #     python_callable=add_function,
    # ).expand(op_args=[[1], [2], [3]])

    # minused_values = minus.partial(y=10).expand(x=[1, 2, 3])

    start>>archiving>>landing>>update_task_runtime>>end