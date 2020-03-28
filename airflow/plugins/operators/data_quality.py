from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.check_operator import CheckOperator, \
    ValueCheckOperator
from airflow.utils.decorators import apply_defaults


class PostgresCheckOperator(CheckOperator):
    """ Performs check against a Postgres database. It expects an SQL query
    and checks if first row of result of query has values which python-eval to
    False. If yes, the operator fails. It passes otherwise.
    """

    @apply_defaults
    def __init__(self,
                 sql,
                 postgres_conn_id,
                 *args, **kwargs):
        """
        Args:
            sql SQL query, ideally resulting in a 1-row result
            postgres_conn_id Conn Id of the database credentials
        Preconditions:
            sql must give non-empty result
        Caution:
            only 1st row of the given SQL query which be checked
        """
        super(PostgresCheckOperator, self).__init__(sql=sql, *args, **kwargs)
        self.postgres_conn_id = postgres_conn_id

    def get_db_hook(self):
        return PostgresHook(self.postgres_conn_id)


class PostgresValueCheckOperator(ValueCheckOperator):
    """ Performs check against a Postgres database. It expects an SQL query
    and checks if the resulting value matches the expected one. If it matches,
    given a tolerance level, the operator passes. Otherwise, it fails.
    """

    @apply_defaults
    def __init__(self,
                 sql,
                 pass_value,
                 postgres_conn_id,
                 tolerance=None,
                 *args, **kwargs):
        """
        Args:
            sql SQL query, ideally resulting in a 1-row, 1-value result
            pass_value expected value
            tolerance amount of error from pass_value to be tolerated
            postgres_conn_id Conn Id of the database credentials
        Preconditions:
            sql must give non-empty result
        Caution:
            only 1st row's 1st value of the given SQL query which be checked
        """
        super(PostgresValueCheckOperator, self).__init__(
            sql=sql,
            pass_value=pass_value,
            tolerance=tolerance,
            *args, **kwargs
        )
        self.postgres_conn_id = postgres_conn_id

    def get_db_hook(self):
        return PostgresHook(self.postgres_conn_id)
