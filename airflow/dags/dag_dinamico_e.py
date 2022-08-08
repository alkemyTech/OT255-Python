from airflow import DAG
import dagfactory


dag_factory = dagfactory.DagFactory("/home/user/airflow/dags/config_e.yml")

dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())