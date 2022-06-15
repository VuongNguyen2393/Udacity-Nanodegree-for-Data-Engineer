from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Check the quality of the data after ingest and loading into target table

    --------------------------
    redshift_conn_id: Redshift connection id
    table: the table which need to check
    primary_key: Primary key of table
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 test_query="",
                 expected_value="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_query = test_query
        self.expected_value = expected_value

    def execute(self, context):
        self.log.info("Get Redshift Connection")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Execute the test query")
        records = redshift.get_records(self.test_query)
        if records[0][0] != self.expected_value:
            raise ValueError(
                f"Check fail at table {self.table}")
        else:
            self.log.info("Check passed")
