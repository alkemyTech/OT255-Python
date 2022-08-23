import logging
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from airflow import DAG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d",
)

# Create custom logger, add console handler
logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)

# -- TASKS --
# development of tasks that take part in the DAG
def _t_query_moron():
    pass


def _t_process_moron():
    pass


def _t_load_moron():
    pass


# -- DAG --
# set up the DAG for the current university
with DAG(
    "dag_univ_moron",
    description="This DAG will process data from Universidad de Morón",
    start_date=datetime(2022, 8, 1),
    schedule_interval=timedelta(hours=1),
    catchup=False,
) as dag:
    # first task: run sql script and export query result to (.csv)?
    task_query_moron = PythonOperator(
        task_id="task_query_moron",
        python_callable=_t_query_moron,
        retries=5,
    )
    # second task: process raw data in pandas
    task_process_moron = PythonOperator(
        task_id="task_process_moron", python_callable=_t_process_moron
    )
    # third task: upload resulting object to amazon s3
    task_load_moron = PythonOperator(
        task_id="task_load_moron", python_callable=_t_load_moron
    )

task_query_moron >> task_process_moron >> task_load_moron
