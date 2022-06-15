from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Loading data into dimensional tables
    --------------------
    redshift_conn_id: Redshift connection id
    table: dimensional table
    sql: SQL command to execute loading
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 append_mode=False,
                 primary_key="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append_mode = append_mode
        self.primary_key = primary_key

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append_mode:
            loading_sql=f"""
                INSERT INTO {self.table}
                {self.sql};
                
                WITH CTE AS (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY {self.primary_key} ORDER BY {self.primary_key}) AS RN
                    FROM {self.table}
                )
                DELETE FROM CTE 
                WHERE RN > 1;
            """
        else:
            self.log.info(f"Truncate data of {self.table}")
            truncate_sql=f"""
                TRUNCATE TABLE {self.table}
                """
            redshift.run(truncate_sql)
            loading_sql=f"""
                INSERT INTO {self.table}
                {self.sql}
                """
        redshift.run(loading_sql)
