from tarfile import TarError
from airflow import DAG
from datetime import timedelta, datetime
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.operators.python import PythonOperator

def conf_logs():
    logging.basicConfig(
        format = '%(asctime)s - %(levelname)s - %(message)s', 
        datefmt = '%Y-%m-%d', level =  logging.INFO
    )

def conex_postgres():
    # Aca defino un PostgresHook para realizar la conexion
    logging.info('Se ha realizado el enlace a Postgres.')
    # Aca realizo la Query con PostgresOperator
    logging.info('Se ha realizado la consulta.')

def manip_pandas():
    # Aca hago todo el procesamiento de la data
    logging.info('Se ha realizado el procesamiento de la informacion.')

def upload_to_s3():
    # Primero establezco la conexion con S3Hook()
    logging.info('Se ha realizado la conexion a S3.')
    # Luego, creo un bucket si no existe todavia con S3CreateBucketOperator
    logging.info('Se ha creado el bucket')
    # Por ultimo, subo el archivo a ese bucket
    logging.info('Se subieron los archivos.')



# Configuracion de los Retries
def_args = {
    'owner': 'Lautaro Flores',
    'retries': 5,
    'retry_delay': timedelta(minutes = 1)
}

# Configuracion del DAG 
with DAG(
    'dag_flores_villamaria',
    default_args = def_args,
    description = 'DAG para las Universidad de Flores y Villa Maria.',
    #Intervalo de ejecución del DAG.
    schedule_interval = '0 * * * *',
    start_date = datetime(2022, 7, 19)
) as dag:
    # Logs

    loggs = PythonOperator(
        task_id = 'loggs',
        python_callable = conf_logs
    )

    # Conexion a la base de datos y extraccion de la data de la universidad
    conexion_postgres = PythonOperator(
        task_id = 'conexion_postgres',
        python_callable = conex_postgres
    )

    # Manipulacion de los datos con Pandas
    process_pandas = PythonOperator(
        task_id = 'extract_pandas',
        python_callable = manip_pandas
    )

    upload_s3 = PythonOperator(
        task_id = 'upload_s3',
        python_callable = upload_to_s3
    )

    loggs >> conexion_postgres >> process_pandas >> upload_s3

    # Info Operators
    """
    - Para la conexion a Postgres
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/_api/airflow/providers/postgres/hooks/postgres/index.html

    - Para la consulta SQL
    from airflow.providers.postgres.operators.postgres import PostgresOperator
    https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/_api/airflow/providers/postgres/operators/postgres/index.html
    
    - Para procesar los datos con Pandas
    from airflow.operators.python import PythonOperator

    - Para conectarse a S3 de Amazon
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/hooks/s3/index.html
    
    - Para crear un Bucket en S3 (en caso de que no exista)
    from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
    https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/operators/s3/index.html

    - Para subir los datos al S3 - Existe un operator para este paso?
    from airflow.operators.python import PythonOperator
    Ejemplo de subida al S3 https://www.youtube.com/watch?v=Gios3wik22Y
"""