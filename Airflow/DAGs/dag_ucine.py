import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.operators.python import PythonOperator

from airflow import DAG

path_root = Path(__file__).parents[1]
sys.path.append(str(path_root))

from src.py_functions import process_univ, query_univ

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
    task_query_univ = PythonOperator(
        task_id=f"task_query_{univ_name}",
        python_callable=query_univ.main(univ_name),
        retries=5,
        retry_delay=timedelta(minutes=2),
    )
    # second task: process raw data in pandas
    task_process_univ = PythonOperator(
        task_id=f"task_process_{univ_name}",
        python_callable=process_univ.main(univ_name),
    )
    # third task: upload resulting object to amazon s3
    task_load_univ = PythonOperator(
        task_id=f"task_load_{univ_name}", python_callable=None
    )

task_query_univ >> task_process_univ >> task_load_univ
