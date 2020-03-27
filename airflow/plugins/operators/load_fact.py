from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    Operator for loading a fact table.
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 database="",
                 table="",
                 variables="",
                 select_clause="",
                 *args, **kwargs):
        """
        Args:
            redshift_conn_id Conn Id of the redshift credentials
            database name of relevant database
            table name of relevant dimension table with database
            select_clause query to get all records to be inserted into the
                dimension table
        """
        # initialize the parent object
        super(LoadFactOperator, self).__init__(*args, **kwargs)

        # store arguments as attributes
        self.redshift_conn_id = redshift_conn_id
        self.table = f"{database}.{table}"
        self.sql_statement = f"INSERT INTO {self.table} {variables} \
            \n{select_clause}"

    def execute(self, context):
        """
        Load fact table.

        Args:
            context provided by airflow

        Returns:
            None
        """
        # get redshift hook
        pg_hook = PostgresHook(self.redshift_conn_id)

        # load the table
        pg_hook.run(self.sql_statement)
        self.log.info(f"Table {self.table} loaded.")
