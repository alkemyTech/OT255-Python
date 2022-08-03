import csv

import pandas
from airflow.providers.postgres.hooks.postgres import PostgresHook


def extraer_datos_univ_nac_de_jujuy():
    cursor_contenido = extraer_info_de_universidad(
        "jujuy_utn", "universidad nacional de jujuy"
    )
    escribir_en_csv("universidad nacional de jujuy", cursor_contenido)


def extraer_datos_con_archivo_sql():
    archivo_sql = open("/home/nadia/airflow/src/sql/univ_nacional_de_jujuy.sql", "r")
    consulta_sql = archivo_sql.read()
    cursor_contenido = conectar_y_obtener_contenido(consulta_sql)
    escribir_en_csv("universidad nacional de jujuy v2", cursor_contenido)


def extraer_info_de_universidad(nombre_tabla, universidad):
    condicion = "university = '" + universidad + "'"
    if "palermo" in universidad:
        condicion = "universidad = '" + universidad + "'"
    consulta_sql = "select * from " + nombre_tabla + " where " + condicion
    cursor_contenido = conectar_y_obtener_contenido(consulta_sql)
    return cursor_contenido


def conectar_y_obtener_contenido(consulta_sql):
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
    universidad = quitar_primer_caracter(universidad)
    archivo_csv = open("/home/nadia/airflow/src/csv/" + universidad + ".csv", "a")
    nombres_de_columnas = obtener_nombres_de_columnas(cursor_contenido) + "\n"
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
    df_codigos_postales = pandas.read_csv("/dags/proyecto_1/csv/codigos_postales.csv")
    print(df_codigos_postales)


def cargar_datos_univ_nac_de_jujuy():
    pass


def extraer_datos_univ_palermo():
    cursor_contenido = extraer_info_de_universidad(
        "palermo_tres_de_febrero", "_universidad_de_palermo"
    )
    escribir_en_csv("_universidad_de_palermo", cursor_contenido)


def quitar_primer_caracter(frase):
    lista_de_palabras = frase.split("_")
    lista_de_palabras.pop(0)
    frase = "_".join(lista_de_palabras)
    return frase


def transformar_datos_univ_palermo():
    pass


def cargar_datos_univ_palermo():
    pass
