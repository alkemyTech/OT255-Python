import csv
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(format="%(asctime)s - %(name)s - %(message)s", datefmt="%Y-%m-%d")

args = {
    "description": "obtener información sobre Univ. Nacional de Jujuy",
    "start_date": datetime(2022, 7, 30),
    "schedule_interval": timedelta(hours=1),
}

dag = DAG("dag_univ_nac_de_jujuy", default_args=args)


def extraer_datos_univ_nac_de_jujuy():
    cursor_contenido = extraer_info_de_universidad(
        "jujuy_utn", "universidad nacional de jujuy"
    )
    escribir_en_csv("universidad nacional de jujuy", cursor_contenido)


def extraer_info_de_universidad(nombre_tabla, universidad):
    consulta_sql = (
        "select * from " + nombre_tabla + "  where university = '" + universidad + "'"
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
    universidad = espacios_por_guiones(universidad)
    archivo_csv = open(
        "/home/nadia/airflow/dags/proyecto_1/csv/" + universidad + ".csv", "a"
    )
    nombres_de_columnas = obtener_nombres_de_columnas(cursor_contenido) + '\n'
    archivo_csv.write(nombres_de_columnas)
    csv.writer(archivo_csv).writerows(cursor_contenido)
    archivo_csv.close()


def espacios_por_guiones(frase_con_espacios):
    for indice in range(len(frase_con_espacios)):
        caracter = frase_con_espacios[indice]
        if caracter == " ":
            frase_con_espacios = frase_con_espacios.replace(" ", "_")
    return frase_con_espacios


def transformar_datos_univ_nac_de_jujuy():
    pass


def cargar_datos_univ_nac_de_jujuy():
    pass


extraer_datos_univ_nac_de_jujuy = PythonOperator(
    task_id="extraer_datos",
    retries=5,
    retry_delay=timedelta(minutes=1),
    python_callable=extraer_datos_univ_nac_de_jujuy,
    dag=dag,
)

transformar_datos_univ_nac_de_jujuy = PythonOperator(
    task_id="transformar_datos",
    python_callable=transformar_datos_univ_nac_de_jujuy,
    dag=dag,
)

cargar_datos_univ_nac_de_jujuy = PythonOperator(
    task_id="cargar_datos", python_callable=cargar_datos_univ_nac_de_jujuy, dag=dag
)

(
    extraer_datos_univ_nac_de_jujuy
    << transformar_datos_univ_nac_de_jujuy
    << cargar_datos_univ_nac_de_jujuy
)
