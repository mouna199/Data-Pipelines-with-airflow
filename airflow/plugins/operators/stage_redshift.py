from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 s3_bucket,
                 s3_prefix,
                 table,
                 redshift_conn_id='redshift',
                 aws_conn_id='aws_credentials',
                 copy_options='',
                 extra_params = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.copy_options = copy_options
        self.extra_params = extra_params
        
    def execute(self, context):
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook("redshift")
        self.log.info(f'Preparing to stage data')

        copy_query = """
                        COPY {table}
                        FROM '{s3_path}'
                        ACCESS_KEY_ID '{access_key}'
                        SECRET_ACCESS_KEY '{secret_key}'
                        REGION 'us-west-2'
                        FORMAT AS JSON '{extra_params}';
                     """.format(table=self.table,
                                s3_path="s3://{}/{}".format(self.s3_bucket,self.s3_prefix),
                                access_key=credentials.access_key,
                                secret_key=credentials.secret_key,
                                extra_params = self.extra_params
                                )

        self.log.info('Copy to table')
        redshift_hook.run(copy_query)
        self.log.info("operation is done.")




