import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook

def logging_dag():
    pass
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
    extract_data1 = PythonOperator(task_id='extract_data1')
    extract_data2 = PythonOperator(task_id='extract_data2')

    #Process data with pandas
    process_data = PythonOperator(task_id='process_data')

    #Load data to S3
    load_data= PythonOperator(task_id='load_data')

    #Task order
    logging_dag >> [extract_data1, extract_data2] >> process_data >> load_data