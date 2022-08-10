from pathlib import Path

import dagfactory

from airflow import DAG

config_file = Path.cwd() / "include/src/dynamic_dag_utn.yaml"
dag_factory = dagfactory.DagFactory(config_file)

dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())
