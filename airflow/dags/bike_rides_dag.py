from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
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

# TODO: load and test staging tables
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

# clear staging tables
[create_fact_n_dims, create_staging_tables] >> end_n_clear_staging
