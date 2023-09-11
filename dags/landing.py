from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from operators.CustomCopy import CopyTableToCsv, CopyFile

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


from airflow.utils.decorators import apply_defaults

from airflow.models.dagrun import DagRun
from airflow.models.dagbag import DagBag

from airflow.utils.task_group import TaskGroup


from airflow.decorators import  task

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
        a=ti.xcom_pull(task_ids="landing.load_enable_task_config"+sub, key="mapped_index")
        if a:
            mapped_index.update(a)
            i+=1
        else: i=-1

    hook = MsSqlHook(mssql_conn_id=staging_con)
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
                        hook.run("EXEC set_task_config {0}, '{1}', '{2}', {3}, '{4}', '{5}'".format(task_id_in_cf_table, task.start_date, task.end_date, task.duration, task.state, task.execution_date))


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
        landing_test_db_db = CopyTableToCsv.partial(
            task_id='landing_test_db_db',
            source_conn_id='source_test_db_db',
            max_active_tis_per_dagrun=2,
        ).expand(task_config=load_enable_task_config('landing_test_db_db'))

        landing_bicycle_retailer_db = CopyTableToCsv.partial(
            task_id='landing_bicycle_retailer_db',
            source_conn_id='source_bicycle_retailer_db',
            max_active_tis_per_dagrun=2,
        ).expand(task_config=load_enable_task_config('landing_bicycle_retailer_db'))

        landing_hrdb_db = CopyTableToCsv.partial(
            task_id='landing_hrdb_db',
            source_conn_id='source_hrdb_db',
            max_active_tis_per_dagrun=2,
        ).expand(task_config=load_enable_task_config('landing_hrdb_db'))

        landing_csv = CopyFile.partial(
            task_id='landing_csv',
            max_active_tis_per_dagrun=2,
        ).expand(task_config=load_enable_task_config('landing_csv'))

        landing_excel = CopyFile.partial(
            task_id='landing_excel',
            max_active_tis_per_dagrun=2,
        ).expand(task_config=load_enable_task_config('landing_excel'))

        landing_json = CopyFile.partial(
            task_id='landing_json',
            max_active_tis_per_dagrun=2,
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