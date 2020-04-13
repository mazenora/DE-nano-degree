from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id,
        self.tables = tables

    def execute(self, context):
        """
            Check if there are rows in list of tables.
            - redshift_conn_id: redshift cluster connection.
            - tables: list of Tables to check.
        """
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                self.log.error(f"Data quality check failed. {table} has No Rows")
                raise ValueError(f"Data quality check failed. {table} has No Rows")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")