import logging
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from include.process_univ import process_univ
from include.query_univ import query_univ

from airflow import DAG

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


def _pass():
    pass


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
    task_query_univ = PythonVirtualenvOperator(
        task_id=f"task_query_{univ_name}",
        python_callable=query_univ,
        op_args=[univ_name],
        requirements=["pandas", "python-dotenv", "sqlalchemy", "psycopg2-binary"],
        retries=5,
        retry_delay=timedelta(minutes=2),
    )
    # second task: process raw data in pandas
    task_process_univ = PythonVirtualenvOperator(
        task_id=f"task_process_{univ_name}",
        python_callable=process_univ,
        op_args=[univ_name],
        requirements=["pandas"]
    )
    # third task: upload resulting object to amazon s3
    task_load_univ = PythonOperator(
        task_id=f"task_load_{univ_name}",
        python_callable=_pass
    )

task_query_univ >> task_process_univ >> task_load_univ
