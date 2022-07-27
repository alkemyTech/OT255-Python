import logging
<<<<<<< HEAD
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import \
    LocalFilesystemToS3Operator

from include.process_univ import process_univ
from include.query_univ import query_univ
=======
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.operators.python import PythonOperator

from airflow import DAG

<<<<<<< HEAD
path_root = Path(__file__).parents[1]
=======
path_root = Path(__file__).parents[2]
>>>>>>> e7887c7 (UC-UBA load scripts as modules for DAG tasks)
sys.path.append(str(path_root))

from src.py_functions import process_univ, query_univ
>>>>>>> d60e4a4 (UC-UBA load scripts as modules for DAG tasks)

# -- INITIAL CONFIG --
# set the name of the university as a variable to simplify code reuse.
univ_name = "ucine"
complete_name = "Universidad del Cine"

# set up configuration for logging to a file and to the console
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(message)s", "%Y-%m-%d")

file_handler = logging.FileHandler(f"dag_univ_{univ_name}.log")
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)

# -- DAG --
# set up the DAG for the current university
with DAG(
    f"dag_univ_{univ_name}",
    description=f"This is the DAG for '{complete_name}'",
    start_date=datetime(2022, 1, 1),
    schedule_interval=timedelta(hours=1),
    catchup=False,
) as dag:
    # first task: run sql script and export query result to (.csv)?
<<<<<<< HEAD
    task_query_univ = PythonVirtualenvOperator(
        task_id=f"task_query_{univ_name}",
        python_callable=query_univ,
        op_args=[univ_name],
        requirements=["pandas", "python-dotenv", "sqlalchemy", "psycopg2-binary"],
=======
    task_query_univ = PythonOperator(
        task_id=f"task_query_{univ_name}",
        python_callable=query_univ(univ_name),
>>>>>>> d60e4a4 (UC-UBA load scripts as modules for DAG tasks)
        retries=5,
        retry_delay=timedelta(minutes=2),
    )
    # second task: process raw data in pandas
<<<<<<< HEAD
    task_process_univ = PythonVirtualenvOperator(
        task_id=f"task_process_{univ_name}",
        python_callable=process_univ,
        op_args=[univ_name],
        requirements=["pandas"]
    )
    # third task: upload resulting object to amazon s3
    task_load_univ = LocalFilesystemToS3Operator(
        task_id=f"task_load_{univ_name}",
        filename=f"./files/modified/g255_{univ_name}.csv",
        aws_conn_id="s3_alkemy",
        dest_key=f"g255_{univ_name}.csv",
        dest_bucket="cohorte-julio-8972766c",
        replace=True,
    )
=======
    task_process_univ = PythonOperator(
        task_id=f"task_process_{univ_name}", python_callable=process_univ(univ_name)
    )
    # third task: upload resulting object to amazon s3
    task_load_univ = PythonOperator(task_id=f"task_load_{univ_name}", python_callable=None)
>>>>>>> d60e4a4 (UC-UBA load scripts as modules for DAG tasks)

task_query_univ >> task_process_univ >> task_load_univ
