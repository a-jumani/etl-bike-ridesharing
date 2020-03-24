from airflow.plugins_manager import AirflowPlugin

from operators.stage_redshift import StageRedshiftOperator


# Defining the plugin class
class CustomPlugin(AirflowPlugin):
    name = "custom_plugin"
    operators = [
        StageRedshiftOperator
    ]
