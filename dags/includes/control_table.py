from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.exceptions import AirflowException
from airflow.models.dagrun import DagRun
from airflow.models.dagbag import DagBag
from airflow.decorators import  task
from airflow.models import Variable

WAREHOUSE_CONN='warehouse_warehouse_db_db'
VOL_MAPPING = Variable.get("VOL_MAPPING", deserialize_json=True)

def replace_from_dict(s, replace_dict):
    try:
        s=s.replace('\\', '/')
        for key, val in replace_dict.items():
            s=s.replace(key, val)
    except AttributeError:
        pass
    return s

def update_task_runtime(dag_id, load_cf_task_id, ti):
    mapped_index={}
    i=0
    while i>=0:
        sub="__"+str(i)
        if i==0:
            sub=""
        a=ti.xcom_pull(task_ids=load_cf_task_id+sub, key="mapped_index")
        if a:
            mapped_index.update(a)
            i+=1
        else: i=-1

    hook = MsSqlHook(mssql_conn_id=WAREHOUSE_CONN)
    dag = DagBag().get_dag(dag_id)
    last_dagrun_run_id = dag.get_last_dagrun(include_externally_triggered=True)

    task_failed=0
    dag_runs = DagRun.find(dag_id=dag_id)
    for dag_run in dag_runs:
    # get the dag_run details for the Dag that triggered this
        if dag_run.execution_date == last_dagrun_run_id.execution_date:
            # get the DAG-level dag_run metadata!
            dag_run_tasks = dag_run.get_task_instances()
            for task in dag_run_tasks:
                try:
                    task_id=task.task_id.split('.')[1]
                except IndexError:
                    print('IndexError')
                    pass
                else:
                    if task_id in mapped_index.keys():
                        task_id_in_cf_table=mapped_index[task_id][str(task.map_index)]
                        # get the TASK-level dag_run metadata!
                        hook.run("EXEC set_task_config {0}, '{1}', '{2}', {3}, '{4}', '{5}'".format(task_id_in_cf_table, task.start_date, task.end_date, task.duration, task.state, task.execution_date))
                        if task.state == 'failed':
                            task_failed=1
    if task_failed:
        raise AirflowException("UPSTEAM TASK FAIL")


@task
def load_enable_task_config(task_name, ti=None):
    """
        return config parameter of every enable child task of a task from task_name
    """
    hook = MsSqlHook(mssql_conn_id=WAREHOUSE_CONN)
    config_df = hook.get_pandas_df(sql="EXEC load_enable_task_config {0}".format(task_name))
    config_df_dict=config_df.to_dict("records")
    
    for i in range(len(config_df_dict)):
        config_df_dict[i]['fetch_data_qr']=hook.get_first('EXEC get_qr_for_select_data_of_task {0}'.format(config_df_dict[i]['task_id']))[0] # indice 0 to get the first col in the row
        config_df_dict[i]['source_location']=replace_from_dict(config_df_dict[i]['source_location'], VOL_MAPPING)
        config_df_dict[i]['target_location']=replace_from_dict(config_df_dict[i]['target_location'], VOL_MAPPING)

    mapped_index={}

    for i in range(len(config_df_dict)):
        if task_name not in mapped_index.keys():
            mapped_index[task_name]={}
        mapped_index[task_name][int(i)]=config_df_dict[i]['task_id']

    ti.xcom_push(key="mapped_index", value=mapped_index)

    return config_df_dict
