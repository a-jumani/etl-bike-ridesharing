from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.custom_plugin import StageRedshiftOperator, \
    LoadDimensionOperator, LoadFactOperator, PostgresCheckOperator, \
    PostgresValueCheckOperator
from sql.sql_queries import SqlInitTables, SqlQueries
from sql.tests import SqlStagingTestsCheck, SqlFactsNDimTestsCheck

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

# task: test holidays staging table
test_staging_holidays = PostgresCheckOperator(
    task_id="Test_staging_holidays",
    dag=dag,
    sql=SqlStagingTestsCheck.check_holidays,
    postgres_conn_id="redshift"
)

# task: test temperature staging table
test_staging_temperature = PostgresCheckOperator(
    task_id="Test_staging_temperature",
    dag=dag,
    sql=SqlStagingTestsCheck.check_temperature,
    postgres_conn_id="redshift"
)

# task: test humidity staging table
test_staging_humidity = PostgresCheckOperator(
    task_id="Test_staging_humidity",
    dag=dag,
    sql=SqlStagingTestsCheck.check_humidity,
    postgres_conn_id="redshift"
)

# task: test weather description staging table
test_staging_weather_desc = PostgresCheckOperator(
    task_id="Test_staging_weather_desc",
    dag=dag,
    sql=SqlStagingTestsCheck.check_weather_desc,
    postgres_conn_id="redshift"
)

# task: test bike rides staging table
test_staging_bike_rides = PostgresValueCheckOperator(
    task_id="Test_staging_bike_rides",
    dag=dag,
    sql=SqlStagingTestsCheck.check_value_bike_rides["query"],
    pass_value=SqlStagingTestsCheck.check_value_bike_rides["value"],
    postgres_conn_id="redshift",
    tolerance=SqlStagingTestsCheck.check_value_bike_rides["tolerance"]
)

# task: dummy staging complete
staging_complete = DummyOperator(
    task_id="Staging_complete",
    dag=dag
)

# task: load time dimension table
load_dim_time = LoadDimensionOperator(
    task_id="Load_dim_time",
    dag=dag,
    redshift_conn_id="redshift",
    database="public",
    table="dim_time",
    select_clause=SqlQueries.load_dim_time,
    empty_table=True
)

# task: load weather description dimension table
load_dim_weather_desc = LoadDimensionOperator(
    task_id="Load_dim_weather_desc",
    dag=dag,
    redshift_conn_id="redshift",
    database="public",
    table="dim_weather_desc",
    variables="(desp)",
    select_clause=SqlQueries.load_dim_weather_desc,
    empty_table=True
)

# task: load station dimension table
load_dim_station = LoadDimensionOperator(
    task_id="Load_dim_station",
    dag=dag,
    redshift_conn_id="redshift",
    database="public",
    table="dim_station",
    variables="(longitude, latitude)",
    select_clause=SqlQueries.load_dim_station,
    empty_table=True
)

# task: load holiday dimension table
load_dim_holiday = LoadDimensionOperator(
    task_id="Load_dim_holiday",
    dag=dag,
    redshift_conn_id="redshift",
    database="public",
    table="dim_holiday",
    select_clause=SqlQueries.load_dim_holiday,
    empty_table=True
)

# task: test weather description dimension table
test_dim_weather_desc = PostgresCheckOperator(
    task_id="Test_dim_weather_desc",
    dag=dag,
    sql=SqlFactsNDimTestsCheck.check_weather_desc,
    postgres_conn_id="redshift"
)

# task: test time dimension table
test_dim_time = PostgresValueCheckOperator(
    task_id="Test_dim_time",
    dag=dag,
    sql=SqlFactsNDimTestsCheck.check_value_time["query"],
    pass_value=SqlFactsNDimTestsCheck.check_value_time["value"],
    postgres_conn_id="redshift",
    tolerance=SqlFactsNDimTestsCheck.check_value_time["tolerance"]
)

# task: test holiday dimension table
test_dim_holiday = PostgresValueCheckOperator(
    task_id="Test_dim_holiday",
    dag=dag,
    sql=SqlFactsNDimTestsCheck.check_value_holidays["query"],
    pass_value=SqlFactsNDimTestsCheck.check_value_holidays["value"],
    postgres_conn_id="redshift",
    tolerance=SqlFactsNDimTestsCheck.check_value_holidays["tolerance"]
)

# task: test station dimension table
test_dim_station = PostgresValueCheckOperator(
    task_id="Test_dim_station",
    dag=dag,
    sql=SqlFactsNDimTestsCheck.check_value_station["query"],
    pass_value=SqlFactsNDimTestsCheck.check_value_station["value"],
    postgres_conn_id="redshift",
    tolerance=SqlFactsNDimTestsCheck.check_value_station["tolerance"]
)

# task: dummy dimensions loaded
dims_loaded = DummyOperator(
    task_id="Dimensions_loaded",
    dag=dag
)

# task: load bike rides fact table
load_fact_bike_rides = LoadFactOperator(
    task_id="Load_fact_bike_rides",
    dag=dag,
    redshift_conn_id="redshift",
    database="public",
    table="fact_bike_rides",
    variables="(customer_id, gender, pickup_time, dropoff_time, \
        pickup_station_id, dropoff_station_id, trip_duration, \
        weather_desc_id, temperature, humidity)",
    select_clause=SqlQueries.load_fact_bike_rides
)

# task: test bike rides fact table
test_fact_bike_rides = PostgresValueCheckOperator(
    task_id="Test_fact_bike_rides",
    dag=dag,
    sql=SqlFactsNDimTestsCheck.check_value_bike_rides["query"],
    pass_value=SqlFactsNDimTestsCheck.check_value_bike_rides["value"],
    postgres_conn_id="redshift",
    tolerance=SqlFactsNDimTestsCheck.check_value_bike_rides["tolerance"]
)

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

# run tests on staging tables
stage_bike_rides_to_redshift >> test_staging_bike_rides
stage_holidays_to_redshift >> test_staging_holidays
stage_humid_to_redshift >> test_staging_humidity
stage_temp_to_redshift >> test_staging_temperature
stage_weather_desc_to_redshift >> test_staging_weather_desc

# staging complete
[test_staging_bike_rides,
 test_staging_holidays,
 test_staging_humidity,
 test_staging_temperature,
 test_staging_weather_desc] >> staging_complete

# load dimensions
[create_fact_n_dims, staging_complete] >> load_dim_time
[create_fact_n_dims, staging_complete] >> load_dim_station
[create_fact_n_dims, staging_complete] >> load_dim_holiday
[create_fact_n_dims, staging_complete] >> load_dim_weather_desc

# test dimensions
load_dim_time >> test_dim_time
load_dim_station >> test_dim_station
load_dim_holiday >> test_dim_holiday
load_dim_weather_desc >> test_dim_weather_desc

# load fact table
[test_dim_time,
 test_dim_station,
 test_dim_holiday,
 test_dim_weather_desc] >> dims_loaded >> load_fact_bike_rides

# test fact table
load_fact_bike_rides >> test_fact_bike_rides

# clear staging tables
test_fact_bike_rides >> end_n_clear_staging
