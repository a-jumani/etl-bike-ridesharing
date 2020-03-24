from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.custom_plugin import StageRedshiftOperator
from sql.sql_queries import SqlInitTables

default_args = {
    "owner": "a-jumani",
    "start_date": datetime(2020, 3, 23),
}

dag = DAG(
    "udac_capstone_dag",
    default_args=default_args,
    description="Load and transform bike rides and weather data in Redshift",
    schedule_interval="@monthly"
)

# task: clear staging area
drop_staging_tables = PostgresOperator(
    task_id="Clear_staging",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlInitTables.drop_staging_tables
)

# task: re-create staging area
create_staging_tables = PostgresOperator(
    task_id="Create_staging",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlInitTables.create_staging_tables
)

# task: create main tables, if needed
create_fact_n_dims = PostgresOperator(
    task_id="Create_fact_n_dims",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlInitTables.create_fact_n_dims_tables
)

# task: load temperature data into staging table
stage_temp_to_redshift = StageRedshiftOperator(
    task_id="Stage_temperature",
    dag=dag,
    table="public.staging_temperature",
    s3_path=Variable.get("temperature_data_path"),
    iam_role=Variable.get("iam_role"),
    redshift_conn_id="redshift",
    json_path=Variable.get("weather_json_path")
)

# task: load humidity data into staging table
stage_humid_to_redshift = StageRedshiftOperator(
    task_id="Stage_humidity",
    dag=dag,
    table="public.staging_humidity",
    s3_path=Variable.get("humidity_data_path"),
    iam_role=Variable.get("iam_role"),
    redshift_conn_id="redshift",
    json_path=Variable.get("weather_json_path")
)

# task: load weather description data into staging table
stage_weather_desc_to_redshift = StageRedshiftOperator(
    task_id="Stage_weather_desc",
    dag=dag,
    table="public.staging_weather_desc",
    s3_path=Variable.get("weather_desc_data_path"),
    iam_role=Variable.get("iam_role"),
    redshift_conn_id="redshift",
    json_path=Variable.get("weather_json_path")
)

# task: load holidays data into staging table
stage_holidays_to_redshift = StageRedshiftOperator(
    task_id="Stage_holidays",
    dag=dag,
    table="public.staging_holiday",
    s3_path=Variable.get("holidays_data_path"),
    iam_role=Variable.get("iam_role"),
    redshift_conn_id="redshift",
    json_path=Variable.get("holidays_json_path")
)

# task: load bike rides data into staging table
stage_bike_rides_to_redshift = StageRedshiftOperator(
    task_id="Stage_bike_rides",
    dag=dag,
    table="public.staging_bike_rides",
    s3_path=Variable.get("bike_rides_data_path"),
    iam_role=Variable.get("iam_role"),
    redshift_conn_id="redshift",
    ext="csv",
    ignore_header=True,
    time_format="YYYY-MM-DD HH:MI:SS"
)

# TODO: test staging tables
# TODO: load and test dimension tables
# TODO: load and test fact table

# task: clear staging area
end_n_clear_staging = PostgresOperator(
    task_id="End_clear_staging",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlInitTables.clear_staging_tables
)

# prepare staging area
drop_staging_tables >> create_staging_tables

# load staging tables
create_staging_tables >> stage_bike_rides_to_redshift
create_staging_tables >> stage_holidays_to_redshift
create_staging_tables >> stage_humid_to_redshift
create_staging_tables >> stage_temp_to_redshift
create_staging_tables >> stage_weather_desc_to_redshift

# clear staging tables
[create_fact_n_dims, create_staging_tables] >> end_n_clear_staging
