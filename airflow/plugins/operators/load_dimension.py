from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Operator for loading a dimension table.
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 database="",
                 table="",
                 variables="",
                 select_clause="",
                 empty_table=False,
                 *args, **kwargs):
        """
        Args:
            redshift_conn_id Conn Id of the redshift credentials
            database name of relevant database
            table name of relevant dimension table within database
            select_clause query to get all records to be inserted into the
                dimension table
            empty_table if set to True, will empty the dimension table first
        """
        # initialize the parent object
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        # store arguments as attributes
        self.redshift_conn_id = redshift_conn_id
        self.table = f"{database}.{table}"
        self.sql_statement = f"INSERT INTO {self.table} {variables} \
            \n{select_clause}"
        self.empty_table = empty_table

    def execute(self, context):
        """
        Load dimension table.

        Args:
            context provided by airflow

        Returns:
            None
        """
        # get redshift hook
        pg_hook = PostgresHook(self.redshift_conn_id)

        # empty the table, if specified
        if self.empty_table:
            pg_hook.run(f"DELETE FROM {self.table}")

        # load the table
        pg_hook.run(self.sql_statement)
        self.log.info(f"Table {self.table} loaded.")
