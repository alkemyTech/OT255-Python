from pathlib import Path

import dagfactory

from airflow import DAG

config_file = f"{str(Path.cwd())}/dags/dag_din_uc-uba.yml"
dag_factory_h = dagfactory.DagFactory(config_file)

dag_factory_h.clean_dags(globals())
dag_factory_h.generate_dags(globals())
