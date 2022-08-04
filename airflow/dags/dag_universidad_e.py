from airflow import DAG
from datetime import  timedelta, datetime
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
import logging
from src.py_functions.extract_e import uInter
from src.py_functions.extract_e  import uPampa
from src.py_functions.clean_e import cleaningData


logger = logging.getLogger('logger')
handlerConsola = logging.StreamHandler() 
logger.addHandler(handlerConsola) 
logger.setLevel(logging.DEBUG)
logging.basicConfig(format='%(asctime)s - %(name)s - %(message)s', datefmt='%Y-%m-%d')
logger.info('Mensaje del log')

default_args_dag={
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'dag_universidadE.py',
    description='Dag Universidades grupo E',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 5, 25),
    default_args=default_args_dag
) as dag:
    consulta_Interamericana = PythonOperator(
        task_id='consultaSQL_UNMoron',
        python_callable=uInter
        )
    consulta_Pampa = PythonOperator(
        task_id='consultaSQL_UNRC',
        python_callable=uPampa        
        )
    procesamientoPandas = PythonOperator(
        task_id='procesamientoPandas',
        python_callable=cleaningData
        )
    carga_pampa =  LocalFilesystemToS3Operator(
        task_id='cargapmpa',
        filename="/files/modified/g255_universidad_lapampa.txt",
        aws_conn_id="s3_conn",
        dest_key= 'g255_universidad_lapampa.txt',
        dest_bucket="*****",
        replace=True
        
    )
    carga_inter =  LocalFilesystemToS3Operator(
        task_id='cargainter',
        filename="/files/modified/g255_universidad_interamericana.txt",
        aws_conn_id="s3_conn",
        dest_key= 'g255_universidad_interamericana.txt',
        dest_bucket="cohorte-julio-8972766c",
        replace=True
        
    )
     

    
     

    [consulta_Interamericana, consulta_Pampa] >> procesamientoPandas >>  [carga_inter, carga_pampa ]