import csv
import pathlib

import pandas

from airflow.providers.postgres.hooks.postgres import PostgresHook


def cargar_datos_univ_nac_de_jujuy():
    pass


def cargar_datos_univ_palermo():
    pass


def extraer_y_escribir_datos_en_csv(universidad):
    ruta_sql = pathlib.Path.cwd() / "src" / "sql" / (universidad + ".sql")
    conexion = PostgresHook(postgres_conn_id="postgres_extraccion").get_conn()
    consulta_sql = open(ruta_sql, "r").read()
    df_contenido = pandas.read_sql(consulta_sql,conexion)
    ruta_csv = pathlib.Path.cwd() / "src" / "csv" / (universidad + ".csv")
    df_contenido.to_csv(ruta_csv,index=False)

def normalizar_datos_de_universidad(universidad):
    ruta = pathlib.Path.cwd() / "src" / "csv" / (universidad + ".csv")
    df_uni = pandas.read_csv(ruta)
    df_uni.age = df_uni.age.apply(int)
    df_uni = df_uni.applymap(quitar_guiones_y_espacios_sobrantes)
    df_uni.gender = df_uni.gender.apply(arreglar_valor_de_genero)
    return df_uni

def normalizar_datos_de_cod_postales():
    ruta_csv = pathlib.Path.cwd() / "src" / "csv" / ("codigos_postales.csv")
    df_codigos_postales = pandas.read_csv(ruta_csv)
    df_codigos_postales = df_codigos_postales.applymap(
        quitar_guiones_y_espacios_sobrantes
    )
    df_codigos_postales = df_codigos_postales.rename(
        columns = {'codigo_postal' : 'postal_code' ,
                   'localidad' : 'location' }
    )
    df_codigos_postales = df_codigos_postales.drop_duplicates()
    return df_codigos_postales


def normalizar_y_escribir_datos_en_txt(universidad):
    df_univ = normalizar_datos_de_universidad(universidad)
    df_codigos_postales = normalizar_datos_de_cod_postales()
    if universidad == "univ_de_palermo" :
        df_univ = df_univ.drop(["location"], axis=1)
        df_univ = pandas.merge(df_univ, df_codigos_postales, on="postal_code")
    else :
        df_univ = df_univ.drop(["postal_code"], axis=1)
        df_univ = pandas.merge(df_univ, df_codigos_postales, on="location")
    df_univ = df_univ.drop_duplicates()
    ruta_txt = pathlib.Path.cwd() / "src" / "txt" / (universidad + ".txt")
    df_univ.to_csv(ruta_txt, index=False)


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
