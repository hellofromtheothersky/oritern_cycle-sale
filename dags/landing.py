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
import os

class Archiving(BashOperator):
    # template_fields = ('bash_command', 'source_file', 'source_dir', 'target_file', 'target_dir')

    @apply_defaults #Looks for an argument named "default_args", and fills the unspecified arguments from it.
    def __init__(
            self,
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


class CopyTableToCsv(BaseOperator):
    """
    load data from table in database to csv file
    """

    def __init__(self, source_conn_id, task_config, *args, **kwargs):
        super().__init__(*args, **kwargs) # initialize the parent operator
        self.source_conn_id = source_conn_id
        self.task_config = task_config


    def convert(self, x):
        if str(x.dtype).startswith('float'):
            try:
                return x.astype('Int64') #http://pandas.pydata.org/pandas-docs/stable/user_guide/integer_na.html
            except:
                return x
        else:
            return x


    # execute() method that runs when a task uses this operator, make sure to include the 'context' kwarg.
    def execute(self, context):
        # write to Airflow task logs
        hook = MsSqlHook(mssql_conn_id=self.source_conn_id)
        csv_dir="{0}/{1}.{2}".format(self.task_config['target_location'], self.task_config['target_table'], self.task_config['target_schema'])

        if not os.path.exists(self.task_config['target_location']):
            os.makedirs(self.task_config['target_location'])

        # self.log.info('COPY TABLE -> FILE ({0}, {1})'.format(table_name, csv_dir))

        df = hook.get_pandas_df(sql=self.task_config['fetch_data_qr'])
        df=df.apply(self.convert)
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
        target_dir="{0}/{1}.{2}".format(task_config['target_location'], task_config['target_table'], task_config['target_schema'])

        # self.log.info('COPY FILE -> FILE ({0}, {1})'.format(source_dir, target_dir)) #not run
        bash_str+="mkdir -p {0}; cp {1} {2};".format(task_config['target_location'], source_dir, target_dir)
            
        super(CopyFile, self).__init__(bash_command=bash_str, *args, **kwargs)


default_args = {
    'owner': 'hieu_nguyen',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}
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
        op_kwargs={'dag_id': 'landing', 'load_cf_task_id': 'load_enable_task_config'},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end = BashOperator(
        task_id='end',
        bash_command="echo END",
        do_xcom_push=False,
    )

    start>>archiving>>landing>>update_task_runtime>>end