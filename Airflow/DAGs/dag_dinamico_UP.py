import pathlib

import dagfactory
from airflow import DAG

archivo_de_configuracion = pathlib.Path.cwd() / "src" / "dag_dinamico_config_UP.yaml"


dag_factory = dagfactory.DagFactory(archivo_de_configuracion)
builtin = globals()
dag_factory.clean_dags(builtin)
dag_factory.generate_dags(builtin)
