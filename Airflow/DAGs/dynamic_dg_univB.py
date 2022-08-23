"""Dynamic DAG for univesidades B -Comahue/salvador-"""
from pathlib import Path
from airflow import DAG
import dagfactory

config_file = Path(__file__).parent / "src" / "yaml" / "conf_univB.yml"
dag_factory_h = dagfactory.DagFactory(config_file)

dag_factory_h.clean_dags(globals())
dag_factory_h.generate_dags(globals())
