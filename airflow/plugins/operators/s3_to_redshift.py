from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class S3ToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 truncate_insert=False,
                 copy_arguments="",
                 create_table="",
                 *args, **kwargs):

        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.truncate_insert = truncate_insert
        self.copy_arguments = copy_arguments
        self.create_table = create_table
        

    def execute(self, context):
        self.log.info(f"Getting AWS Hook for { self.aws_credentials_id }. ")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        self.log.info(f"Getting Postgres Hook for { self.redshift_conn_id }. ")
        self.log.info('GET Redshift Hook')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_insert:
            self.log.info(f"Truncate-Insert Mode is activated. Dropping { self.table } table if exists. ")
            redshift.run(f"DROP TABLE IF EXISTS { self.table }")
        
        self.log.info(f"Creating { self.table } table if not exists using the provided schema. ")
        redshift.run(self.create_table)
        
        self.log.info(f"Copy data from S3 into { self.table } table. ")
        self.s3_key = self.s3_key.format(year = context.get('execution_date').strftime("%Y"), 
                                         month = context.get('execution_date').strftime("%m"),
                                         day = context.get('execution_date').strftime("%d"),
                                         date = context.get('execution_date').strftime("%Y-%m-%d"))
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = S3ToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.copy_arguments
        )
        redshift.run(formatted_sql)
        
        self.log.info(f"Finished { self.table } table. ")
        