import csv
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(format="%(asctime)s - %(name)s - %(message)s", datefmt="%Y-%m-%d")

args = {
    "description": "obtener información sobre Univ. de Palermo",
    "start_date": datetime(2022, 7, 30),
    "schedule_interval": timedelta(hours=1),
}

dag = DAG("dag_univ_palermo", default_args=args)


def extraer_datos_univ_palermo():
    cursor_contenido = extraer_info_de_universidad(
        "palermo_tres_de_febrero", "_universidad_de_palermo"
    )
    escribir_en_csv("_universidad_de_palermo", cursor_contenido)


def extraer_info_de_universidad(nombre_tabla, universidad):
    consulta_sql = (
        "select * from " + nombre_tabla + "  where universidad = '" + universidad + "'"
    )
    hook = PostgresHook(postgres_conn_id="postgres_extraccion")
    conexion = hook.get_conn()
    cursor_contenido = conexion.cursor()
    cursor_contenido.execute(consulta_sql)
    return cursor_contenido


def obtener_nombres_de_columnas(cursor_contenido):
    lista_de_nombres = [columna[0] for columna in cursor_contenido.description]
    nombres_separados_por_comas = ",".join(lista_de_nombres)
    return nombres_separados_por_comas


def escribir_en_csv(universidad, cursor_contenido):
    universidad = quitar_primer_caracter(universidad)
    archivo_csv = open(
        "/home/nadia/airflow/dags/proyecto_1/csv/" + universidad + ".csv", "a"
    )
    nombres_de_columnas = obtener_nombres_de_columnas(cursor_contenido) + "\n"
    archivo_csv.write(nombres_de_columnas)
    csv.writer(archivo_csv).writerows(cursor_contenido)
    archivo_csv.close()


def quitar_primer_caracter(frase):
    lista_de_palabras = frase.split("_")
    lista_de_palabras.pop(0)
    frase = "_".join(lista_de_palabras)
    return frase


def transformar_datos_univ_palermo():
    pass


def cargar_datos_univ_palermo():
    pass


extraer_datos_univ_palermo = PythonOperator(
    task_id="extraer_datos",
    retries=5,
    retry_delay=timedelta(minutes=1),
    python_callable=extraer_datos_univ_palermo,
    dag=dag,
)

transformar_datos_univ_palermo = PythonOperator(
    task_id="transformar_datos", python_callable=transformar_datos_univ_palermo, dag=dag
)

cargar_datos_univ_palermo = PythonOperator(
    task_id="cargar_datos", python_callable=cargar_datos_univ_palermo, dag=dag
)

(
    extraer_datos_univ_palermo
    << transformar_datos_univ_palermo
    << cargar_datos_univ_palermo
)
