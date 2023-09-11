from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from airflow.models import BaseOperator

from airflow.models.dagrun import DagRun
from airflow.models.dagbag import DagBag

from airflow.utils.task_group import TaskGroup


from airflow.decorators import task

from airflow.utils.trigger_rule import TriggerRule


from datetime import datetime
staging_con='staging_staging_db_db'

@task
def load_enable_task_config(task_name, ti=None):
    """
        return config parameter of every enable child task of a task from task_name
    """
    hook = MsSqlHook(mssql_conn_id=staging_con)
    config_df = hook.get_pandas_df(sql="EXEC load_enable_task_config {0}".format(task_name))
    config_df_dict=config_df.to_dict("records")
    
    for i in range(len(config_df_dict)):
        config_df_dict[i]['fetch_data_qr']=hook.get_first('EXEC get_qr_for_select_data_of_task {0}'.format(config_df_dict[i]['task_id']))[0] # indice 0 to get the first col in the row

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
        a=ti.xcom_pull(task_ids="staging.load_enable_task_config"+sub, key="mapped_index")
        if a:
            mapped_index.update(a)
            i+=1
        else: i=-1

    hook = MsSqlHook(mssql_conn_id=staging_con)
    dag = DagBag().get_dag('staging')
    last_dagrun_run_id = dag.get_last_dagrun(include_externally_triggered=True)

    dag_runs = DagRun.find(dag_id='staging')
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
                        hook.run("EXEC set_task_config {0}, '{1}', '{2}', {3}, '{4}', '{5}'".format(task_id_in_cf_table, task.start_date, task.end_date, task.duration, task.state, task.execution_date))


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
        is_incre=0
        if self.task_config['is_incre']:
            is_incre=self.task_config['is_incre']
        hook.run("EXEC load_to_stage_table '{0}', '{1}', {2}".format(
            self.task_config['source_location'].replace('/opt/airflow/landing_cycle-sale/testdb/', 'C:\\temp\\cycle-sale\\testdb\\'),
            self.task_config['target_table'], 
            is_incre
            ))


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

    start>>staging>>update_task_runtime>>end