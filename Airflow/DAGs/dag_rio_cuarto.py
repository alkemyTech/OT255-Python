from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


# -- TASKS --
# development of tasks that take part in the DAG
def _t_query_rio_cuarto():
    pass


def _t_process_rio_cuarto():
    pass


def _t_load_rio_cuarto():
    pass


# -- DAG --
# set up the DAG for the current university
with DAG(
    "dag_rio_cuarto",
    description="This is the DAG for 'Universidad De Río Cuarto'",
    start_date=datetime(2022, 1, 1),
    schedule_interval=timedelta(hours=1),
    catchup=False,
) as dag:
    # first task: run sql script and export query result to (.csv)?
    task_query_rio_cuarto = PythonOperator(
        task_id="task_query_rio_cuarto",
        python_callable=_t_query_rio_cuarto,
        retries=5,
    )
    # second task: process raw data in pandas
    task_process_rio_cuarto = PythonOperator(
        task_id="task_process_rio_cuarto", python_callable=_t_process_rio_cuarto
    )
    # third task: upload resulting object to amazon s3
    task_load_rio_cuarto = PythonOperator(
        task_id="task_load_rio_cuarto", python_callable=_t_load_rio_cuarto
    )

task_query_rio_cuarto >> task_process_rio_cuarto >> task_load_rio_cuarto
