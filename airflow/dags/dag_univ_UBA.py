import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# -- INITIAL CONFIG --
# set up configuration for logging to a file and to the console
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(message)s", "%Y-%m-%d")

file_handler = logging.FileHandler("dag_univ_UBA.log")
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)


# -- TASKS --
# development of tasks that take part in the DAG
def _t_query_uba():
    pass


def _t_process_uba():
    pass


def _t_load_uba():
    pass


# -- DAG --
# set up the DAG for the current university
with DAG(
    "dag_univ_uba",
    description="This is the DAG for 'Universidad de Buenos Aires'",
    start_date=datetime(2022, 1, 1),
    schedule_interval=timedelta(hours=1),
    catchup=False,
) as dag:
    # first task: run sql script and export query result to (.csv)?
    task_query_uba = PythonOperator(
        task_id="task_query_uba",
        python_callable=_t_query_uba,
        retries=5,
        retry_delay=timedelta(minutes=2),
    )
    # second task: process raw data in pandas
    task_process_uba = PythonOperator(
        task_id="task_process_uba", python_callable=_t_process_uba
    )
    # third task: upload resulting object to amazon s3
    task_load_uba = PythonOperator(task_id="task_load_uba", python_callable=_t_load_uba)

task_query_uba >> task_process_uba >> task_load_uba
