from airflow.plugins_manager import AirflowPlugin

from operators.stage_redshift import StageRedshiftOperator
from operators.data_quality import PostgresCheckOperator


# Defining the plugin class
class CustomPlugin(AirflowPlugin):
    name = "custom_plugin"
    operators = [
        StageRedshiftOperator,
        PostgresCheckOperator
    ]
