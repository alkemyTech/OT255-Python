from pathlib import Path

import dagfactory

from airflow import DAG

config_file = Path.cwd() / "dynamic_dag_3feb.yaml"
dag_factory = dagfactory.DagFactory(config_file)

dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())
