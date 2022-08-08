import csv
import pathlib

import pandas

from airflow.providers.postgres.hooks.postgres import PostgresHook


def extraer_datos_univ_nac_de_jujuy():
    nombre_universidad = "univ_nacional_de_jujuy"
    extraer_y_escribir_datos_en_csv(nombre_universidad)


def extraer_datos_univ_palermo():
    nombre_universidad = "univ_de_palermo"
    extraer_y_escribir_datos_en_csv(nombre_universidad)


def transformar_datos_univ_nac_de_jujuy():
    universidad = "univ_nacional_de_jujuy"
    normalizar_y_escribir_datos_en_txt(universidad)


def transformar_datos_univ_palermo():
    universidad = "univ_de_palermo"
    normalizar_y_escribir_datos_en_txt(universidad)


def cargar_datos_univ_nac_de_jujuy():
    pass


def cargar_datos_univ_palermo():
    pass


def extraer_y_escribir_datos_en_csv(universidad):
    ruta_sql = pathlib.Path.cwd() / "src" / "sql" / (universidad + ".sql")
    consulta_sql = open(ruta_sql, "r").read()
    conexion = PostgresHook(postgres_conn_id="postgres_extraccion").get_conn()
    cursor_contenido = conexion.cursor()
    cursor_contenido.execute(consulta_sql)
    ruta_csv = pathlib.Path.cwd() / "src" / "csv" / (universidad + ".csv")
    archivo_csv = open(ruta_csv, "a")
    lista_de_nombres = [columna[0] for columna in cursor_contenido.description]
    nombres_separados_por_comas = ",".join(lista_de_nombres) + "\n"
    archivo_csv.write(nombres_separados_por_comas)
    csv.writer(archivo_csv).writerows(cursor_contenido)
    archivo_csv.close()


def normalizar_y_escribir_datos_en_txt(universidad):
    ruta = pathlib.Path.cwd() / "src" / "csv" / (universidad + ".csv")
    df_uni = pandas.read_csv(ruta)
    df_uni.age = df_uni.age.apply(int)
    df_uni = df_uni.applymap(quitar_guiones_y_espacios_sobrantes)
    df_uni.gender = df_uni.gender.apply(arreglar_valor_de_genero)
    for indice, localidad in df_uni.location.items():
        codigo_postal = obtener_codigo_postal_segun_localidad(localidad)
        df_uni.at[indice, "postal_code"] = codigo_postal
    datos_como_string = df_uni.to_string(index=False)
    ruta_txt = pathlib.Path.cwd() / "src" / "txt" / (universidad + ".txt")
    open(ruta_txt, "a").write(datos_como_string)


def quitar_guiones_y_espacios_sobrantes(valor):
    valor = str(valor).lower()
    valor = valor.replace("_", " ")
    valor = valor.strip("_- ")
    return valor


def arreglar_valor_de_genero(valor):
    if valor == "f":
        valor = "female"
    else:
        valor = "male"
    return valor


def obtener_codigo_postal_segun_localidad(localidad):
    ruta_csv = pathlib.Path.cwd() / "src" / "csv" / ("codigos_postales.csv")
    df_codigos_postales = pandas.read_csv(ruta_csv)
    df_codigos_postales = df_codigos_postales.applymap(
        quitar_guiones_y_espacios_sobrantes
    )
    df_codigos_postales = df_codigos_postales.drop_duplicates()
    condicion = df_codigos_postales.localidad.str.contains(localidad)
    df_codigos_postales = df_codigos_postales[condicion]
    codigo_postal = df_codigos_postales.codigo_postal.iloc[0]
    return codigo_postal
