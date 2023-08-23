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

def tables_to_csv_files(hook):
    tables_name=hook.get_records(sql="""	SELECT CONCAT(TABLE_SCHEMA, '.', TABLE_NAME)
                                        FROM INFORMATION_SCHEMA.TABLES
                                        WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG='BicycleRetailer' and TABLE_SCHEMA!='dbo'""")
    for table_name in tables_name:
        table_name=table_name[0]
        print('Table to csv for '+table_name)
        sql="SELECT * from {0}".format(table_name)
        df = hook.get_pandas_df(sql=sql)
        df.to_csv('/opt/airflow/landing_cycle-sale/{0}.csv'.format(table_name), index=False)

def able_to_run_task(task_name):
    hook_config_table = MsSqlHook(mssql_conn_id='staging_db')
    tuple=hook_config_table.get_records(sql="""
                                        declare @able_to_run bit
                                        EXEC able_to_run '{}', @able_to_run output
                                        select @able_to_run
                                        """.format(task_name))
    return tuple[0][0]


def landing_bicycle_retailer_db():
    # con=BaseHook.get_connection("source_bicycle_retailer_db").get_uri()
    # con+='?driver=ODBC+Driver+17+for+SQL+Server'
    # df =  pd.read_sql('SELECT * from [Production].[Product]', con)
    # print(df.head())
    can_run=able_to_run_task('landing_bicycle_retailer_db')
    if can_run == False :
        print("REFUSE TO RUN FROM CONFIG TABLE")
    else:
        hook = MsSqlHook(mssql_conn_id='source_bicycle_retailer_db')
        # hook = MsSqlHook(mssql_conn_id='AAA')
        tables_to_csv_files(hook)


def landing_hrdb_db():
    can_run=able_to_run_task('landing_hrdb_db')
    if can_run == False :
        print("REFUSE TO RUN FROM CONFIG TABLE")
    else:
        hook = MsSqlHook(mssql_conn_id='source_hrdb_db')
        tables_to_csv_files(hook)


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

    landing_csv = BashOperator(
        task_id='landing_csv',
        bash_command="cp /opt/airflow/source/Csv/*.csv /opt/airflow/landing_cycle-sale"
    )

    
    landing_excel = BashOperator(
        task_id='landing_excel',
        bash_command="cp /opt/airflow/source/Excel/*.xlsx /opt/airflow/landing_cycle-sale"
    )

    landing_json = BashOperator(
        task_id='landing_json',
        bash_command="cp /opt/airflow/source/Json/*.json /opt/airflow/landing_cycle-sale"
    )

    # landing_bicycle_retailer_db = MsSqlOperator(
    #     task_id='landing_bicycle_retailer_db',
    #     mssql_conn_id="source_bicycle_retailer_db",
    #     sql=r"""select top 5 * from [Sales].[CreditCard]""",
    # )

    # @dag.task(task_id="copy data")
    # def copy_mssql_hook():
    #     mssql_hook = MsSqlHook(mssql_conn_id="source_bicycle_retailer_db")
    #     df =  pd.read_sql('SELECT *', Connnection.get("my_connection_id").get_uri()) 

    landing_bicycle_retailer_db = PythonOperator(
        task_id='landing_bicycle_retailer_db',
        python_callable=landing_bicycle_retailer_db,

    )

    landing_hrdb_db = PythonOperator(
        task_id='landing_hrdb_db',
        python_callable=landing_hrdb_db,
    )

    end = BashOperator(
        task_id='end',
        bash_command="echo END",
    )


    start>>[landing_csv, landing_excel, landing_json, landing_bicycle_retailer_db, landing_hrdb_db]>>end
