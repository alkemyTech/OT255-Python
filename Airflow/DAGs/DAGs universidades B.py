"""DAG created for processing data from postgres using pandas and loading it to Amazon S3"""
import logging
from datetime import datetime

import airflow
from airflow.models import Variable
from airflow.operators import python_operator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

# Logger configuration
logging.basicConfig(
    level=logging.INFO,
    format='%Y-%m-%d - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler('univ_b.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

postgres_conn_id = Variable.get('postgres_conn_id')
s3_bucket = Variable.get('amazon_bucket')
s3_key = Variable.get('amazon_key')


def postgres():
    """Python function to extract queries"""
    pass


def pandas_processing():
    """Python function for pandas processing"""
    pass

logging.info('DAG para univ del Salvador y Comahue')
with airflow.DAG(
        'dag_univ_salvador_comahue',
        description='DAG para univ del Salvador y Comahue',
        schedule_interval='@hourly',
        start_date=datetime(2022, 7, 17)
) as dag:

    logging.info('Extracting sql archives...')
    # Here we extract sql archives for further use
    extract_sql = python_operator.PythonOperator(
        task_id='extract_sql',
        python_callable=postgres
    )

    logging.info('Performing queries...')
    # Here we perform the queries
    with TaskGroup(group_id='queries') as queries:
        run_queries = PostgresOperator(
            task_id='run_salvador',
            postgres_conn_id=postgres_conn_id,
            sql='path to sql',
            retries = 5
        )

        run_queries_2 = PostgresOperator(
            task_id='run_comahue',
            postgres_conn_id=postgres_conn_id,
            sql='path to sql',
            retries = 5
        )

        run_queries >> run_queries_2

    logging.info('Processing with pandas...')
    # Python processing with pandas
    pandas_processing = python_operator.PythonOperator(
        task_id='pandas',
        python_callable=pandas_processing()
    )

    logging.info('Storing results...')
    # Here we store our processed data in amazon aws
    aws_bucket = S3CreateObjectOperator(
        task_id='s3_create_object',
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        replace=True,
        retries = 5
    )

    extract_sql >> queries >> pandas_processing >> aws_bucket
