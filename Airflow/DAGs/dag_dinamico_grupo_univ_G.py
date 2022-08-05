import dagfactory
from airflow import DAG

config_file = "/usr/local/airflow/dags/dag_dinamico_grupo_univ_G.yml"
example_dag_factory = dagfactory.DagFactory(config_file)

# Creating task dependencies
example_dag_factory.clean_dags(globals())
example_dag_factory.generate_dags(globals())
