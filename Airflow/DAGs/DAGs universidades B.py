"""DAG created for processing data from postgres using pandas and loading it to Amazon S3"""
import airflow
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from airflow.operators import python_operator
from airflow.utils.task_group import TaskGroup
from src.py_functions import queryDatabase
from src.py_functions import pandas_comahue
from src.py_functions import pandas_salvador
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

# Logger configuration
logging.basicConfig(
    level=logging.INFO,
    format='%Y-%m-%d - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler('univ_b.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Loading variables from dotenv
load_dotenv()

postgres_conn_id = os.getenv('postgres_conn_id')
s3_bucket = os.getenv('s3_bucket')
s3_key = os.getenv('s3_key')
path_c = os.getenv('path_c')
path_s = os.getenv('path_s')
auth = os.getenv('auth')
destFolder = os.getenv('destFolder')

logging.info('DAG para univ del Salvador y Comahue')
with airflow.DAG(
        'dag_univ_salvador_comahue',
        description='DAG para univ del Salvador y Comahue',
        schedule_interval='@hourly',
        start_date=datetime(2022, 7, 17)
) as dag:
    logging.info('Querying database...')
    # Here we extract sql archives for further use
    extract_sql = python_operator.PythonOperator(
        task_id='query_database',
        python_callable=queryDatabase,
        retries=5
    )

    logging.info('Processing with pandas...')
    # Here we perform the queries
    with TaskGroup(group_id='queries') as pandas_processing:
        pandas_processing_comahue = python_operator.PythonOperator(
            task_id='pandas_comahue',
            python_callable=pandas_comahue
        )

        pandas_processing_salvador = python_operator.PythonOperator(
            task_id='pandas_salvador',
            python_callable=pandas_salvador
        )

        pandas_processing_comahue >> pandas_processing_salvador

    logging.info('Storing results...')
    # Here we store our processed data in amazon aws
    aws_bucket = S3CreateObjectOperator(
        task_id='s3_create_object',
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        replace=True,
        retries=5
    )

    extract_sql >> pandas_processing >> aws_bucket
