from airflow.plugins_manager import AirflowPlugin
import operators

#Define the plugin class
class MyPlugins(AirflowPlugin):
    name = "my_plugin"
    
    operators = [operators.CopyTableToCsv, operators.CopyFile]
    tasks=[tasks.load_enable_task_config]