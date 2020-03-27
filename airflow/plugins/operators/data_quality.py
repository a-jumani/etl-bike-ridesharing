from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
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


class PostgresGenericCheckOperator(BaseOperator):
    """
    Operator for executing quality checks on data pipelines' end data
    in a Postgres database.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 db_conn_id,
                 tests={},
                 *args, **kwargs):
        """
        Args:
            db_conn_id Conn Id of the database credentials
            tests sequence of dicts of format
                {"query": "<SQL query>", "expected": <val>}
        Preconditions:
            <SQL query> must give non-empty result
        Caution:
            only 1st value from 1st row of results will be used for comparison
        """
        # initialize the parent object
        super(PostgresGenericCheckOperator, self).__init__(*args, **kwargs)

        # store arguments as attributes
        self.db_conn_id = db_conn_id
        self.tests = tests

    def execute(self, context):
        """
        Execute the test queries.

        Args:
            context provided by airflow
        Returns:
            None
        """
        # get db hook
        pg_hook = PostgresHook(self.db_conn_id)

        # set variable to denote test failure
        passed = True

        # execute all queries and get results
        for test in self.tests:

            q, r = test["query"], test["expected"]
            ret = pg_hook.get_records(test["query"])

            # no records found
            if len(ret) < 1 or len(ret[0]) < 1:
                self.log.error(
                    f"Query {q} gave no result but {r} was expected")
                passed = False

            # record doesn't match the expected result
            elif str(ret[0][0]) != str(test["expected"]):
                self.log.error(
                    f"Query {q} gave result {ret[0][0]} when {r} was expected")
                passed = False

            # record matches expectations
            else:
                self.log.info(
                    f"Query {q} returned {r}, the expected result")

        # signal failure
        if not passed:
            raise ValueError("Some tests failed. Check logs for details.")

        # log success
        else:
            self.log.info(f"All {len(self.queries)} tests passed")
