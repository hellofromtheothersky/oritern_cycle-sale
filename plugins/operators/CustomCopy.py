from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.models import BaseOperator
from airflow.operators.bash import BashOperator
from airflow.utils.decorators import apply_defaults

import os

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
        csv_dir="{0}{1}.{2}".format(self.task_config['target_location'], self.task_config['target_table'], self.task_config['target_schema'])

        if not os.path.exists(self.task_config['target_location']):
            os.makedirs(self.task_config['target_location'])

        # self.log.info('COPY TABLE -> FILE ({0}, {1})'.format(table_name, csv_dir))

        df = hook.get_pandas_df(sql=self.task_config['fetch_data_qr'])
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