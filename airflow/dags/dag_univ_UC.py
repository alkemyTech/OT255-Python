from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime
import logging


# -- INITIAL CONFIG --
# set up configuration for logging to a file and to the console
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(message)s", "%Y-%m-%d")

file_handler = logging.FileHandler("dag_univ_UC.log")
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)


# -- TASKS --
# development of tasks that take part in the DAG
def _t_query_uc():
    pass

def _t_process_uc():
    pass

def _t_load_uc():
    pass


# -- DAG --
# set up the DAG for the current university
with DAG(
    "dag_univ_uc",
    description = "This is the DAG for 'Universidad del Cine'",
    start_date = datetime(2022, 1, 1),
    schedule_interval = timedelta(hours=1),
    catchup = False
) as dag:
    # first task: run sql script and export query result to (.csv)?
    task_query_uc = PythonOperator(
        task_id = "task_query_uc",
        python_callable = _t_query_uc,
        retries = 5,
        retry_delay = timedelta(minutes=2)
        )
    # second task: process raw data in pandas
    task_process_uc = PythonOperator(
        task_id = "task_process_uc",
        python_callable = _t_process_uc
        )
    # third task: upload resulting object to amazon s3
    task_load_uc = PythonOperator(
        task_id = "task_load_uc",
        python_callable = _t_load_uc
        )

task_query_uc >> task_process_uc >> task_load_uc