from airflow.plugins_manager import AirflowPlugin

from operators.stage_redshift import StageRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import PostgresCheckOperator, \
    PostgresValueCheckOperator, PostgresGenericCheckOperator


# Defining the plugin class
class CustomPlugin(AirflowPlugin):
    name = "custom_plugin"
    operators = [
        StageRedshiftOperator,
        LoadFactOperator,
        LoadDimensionOperator,
        PostgresCheckOperator,
        PostgresValueCheckOperator,
        PostgresGenericCheckOperator
    ]
