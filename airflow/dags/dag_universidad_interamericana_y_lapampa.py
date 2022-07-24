
import logging
from datetime import datetime, timedelta

import airflow
from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

postgres_conn_id = Variable.get('postgres_conn_id')
s3_bucket = Variable.get('amazon_bucket')
s3_key = Variable.get('amazon_key')


logging.basicConfig(filename='', filemode='', format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%d-%m-%y', level=logging.INFO)
logger = logging.getLogger(__name__)



def logging_dag():
    logger.info('Iniciando DAG')
def extract_data1():
    pass
def extract_data2():
    pass
def process_data():
    pass
def load_data():
    pass

default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'universidad_interamericana_y_lapampa',   
    description= 'Procesa la información de las Facultades',
    default_args = default_args,
    schedule_interval= timedelta(hours=1),  
    start_date = datetime(2022, 7, 18)  
)as dag:
   
    #Initializing dags
    logging_dag = PythonOperator(task_id='logging_dag')

    #Extract data SQL query to Postgres database
    extract_data = PythonOperator(task_id='extract_data1')
    

    #Process data with pandas
    process_data = PythonOperator(task_id='process_data')

    #Load data to S3
    load_data= PythonOperator(task_id='load_data')

    #Task order
    logging_dag >> [extract_data1] >> process_data >> load_data