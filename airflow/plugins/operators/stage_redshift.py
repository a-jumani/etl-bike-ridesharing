from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageRedshiftOperator(BaseOperator):
    """
    Operator for loading staging tables in AWS Redshift from S3 using common
    prefix method. Only json and delimited files are supported.
    """

    # allow s3 path to be templated
    template_fields = ("s3_path",)

    ui_color = '#358140'

    # template of COPY query
    copy_sql = """
        COPY {table} FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        {data_format}
        {time_format}
        BLANKSASNULL
        EMPTYASNULL;
    """

    @apply_defaults
    def __init__(self,
                 table="",
                 s3_path="",
                 iam_role="",
                 redshift_conn_id="",
                 ext="json",
                 json_path=None,
                 delim=',',
                 ignore_header=False,
                 time_format=None,
                 *args, **kwargs):
        """
        Args:
            table name of relevant dimension table with database
            s3_path path to files to be loaded in s3; can be templated
            iam_role aws role arn
            redshift_conn_id Conn Id of the redshift credentials
            compression compression technique (e.g. gzip) if data is compressed
            ext file extension
            json_path path to json formatting; use if ext is set to "json";
                uses 'auto' when not specified
            delim delimiter; use when ext is not set to "json"
            time_format time format to use for TIMESTAMP fields
        """
        # initialize the parent object
        super(StageRedshiftOperator, self).__init__(*args, **kwargs)

        # store arguments as attributes
        self.table = table
        self.s3_path = s3_path
        self.iam_role = iam_role
        self.redshift_conn_id = redshift_conn_id

        # json file
        if ext == "json":
            self.data_format = "FORMAT AS JSON 'auto'" if json_path is None \
                else "FORMAT AS JSON '{}'".format(json_path)

        # delimited file
        else:
            self.data_format = "DELIMITER '{}'".format(delim)
            if ignore_header:
                self.data_format += " IGNOREHEADER 1"

        # apply time format
        if time_format is not None:
            self.time_format = "TIMEFORMAT '{}'".format(time_format)
        else:
            self.time_format = ''

    def execute(self, context):
        """
        Load staging table.

        Args:
            context provided by airflow

        Returns:
            None
        """
        # get redshift hook
        pg_hook = PostgresHook(self.redshift_conn_id)

        # create the sql statement using statement
        sql_statement = StageRedshiftOperator.copy_sql.format(
            table=self.table,
            s3_path=self.s3_path,
            iam_role=self.iam_role,
            data_format=self.data_format,
            time_format=self.time_format
        )

        # load the statement
        pg_hook.run(sql_statement)
        self.log.info(f"Copy to {self.table} from {self.s3_path} completed")
