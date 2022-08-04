import csv

import pandas
from airflow.providers.postgres.hooks.postgres import PostgresHook


def extraer_datos_univ_nac_de_jujuy():
    nombre_universidad = "universidad nacional de jujuy"
    cursor_contenido = extraer_datos_con_archivo_sql(nombre_universidad)
    escribir_en_csv(nombre_universidad, cursor_contenido)


def extraer_datos_univ_nac_de_jujuy_v2():
    cursor_contenido = extraer_info_de_universidad(
        "jujuy_utn", "universidad nacional de jujuy"
    )
    escribir_en_csv("universidad nacional de jujuy", cursor_contenido)


def extraer_datos_univ_palermo():
    nombre_universidad = "universidad de palermo"
    cursor_contenido = extraer_datos_con_archivo_sql(nombre_universidad)
    escribir_en_csv(nombre_universidad, cursor_contenido)


def extraer_datos_con_archivo_sql(universidad):
    universidad = espacios_por_guiones(universidad)
    universidad = universidad.replace("universidad", "univ")
    ruta = "/home/nadia/airflow/src/sql/" + universidad + ".sql"
    archivo_sql = open(ruta, "r")
    consulta_sql = archivo_sql.read()
    cursor_contenido = conectar_y_obtener_contenido(consulta_sql)
    return cursor_contenido


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
    universidad = universidad.lstrip("_")
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


def normalizar_universidad(nombre_universidad):
    nombre_universidad = espacios_por_guiones(nombre_universidad)
    ruta = "/home/nadia/airflow/src/csv/" + nombre_universidad + ".csv"
    df_universidad = pandas.read_csv(ruta)
    df_universidad = corregir_formato(df_universidad)
    return df_universidad


def corregir_formato(df_universidad):
    df_universidad["age"] = df_universidad["age"].apply(int)
    df_universidad = df_universidad.applymap(quitar_guiones_y_espacios_sobrantes)
    df_universidad["gender"] = df_universidad["gender"].apply(arreglar_valor_de_genero)
    return df_universidad


def normalizar_codigos_postales():
    ruta = "/home/nadia/airflow/src/csv/codigos_postales.csv"
    df_codigos_postales = pandas.read_csv(ruta)
    df_codigos_postales = df_codigos_postales.applymap(
        quitar_guiones_y_espacios_sobrantes
    )
    return df_codigos_postales


def obtener_codigo_postal(df_codigos_postales, localidad):
    condicion = df_codigos_postales.localidad.str.contains(localidad)
    df_codigos_postales = df_codigos_postales[condicion]
    codigo_postal = df_codigos_postales.codigo_postal.iloc[0]
    return codigo_postal


def corregir_codigos_postales(df_universidad):
    df_codigos_postales = normalizar_codigos_postales()
    df_codigos_postales = df_codigos_postales.drop_duplicates()
    for indice, localidad in df_universidad.location.items():
        codigo_postal = obtener_codigo_postal(df_codigos_postales, localidad)
        df_universidad.at[indice, "postal_code"] = codigo_postal
    return df_universidad


def escribir_en_archivo_txt(df_universidad, iniciales_uni):
    ruta = "/home/nadia/airflow/src/txt/datos_normalizados_" + iniciales_uni + ".txt"
    archivo_txt = open(ruta, "a")
    datos_como_string = df_universidad.to_string(index=False)
    archivo_txt.write(datos_como_string)


def transformar_datos_univ_nac_de_jujuy():
    df_univ_nac_de_jujuy = normalizar_universidad("universidad nacional de jujuy")
    df_univ_nac_de_jujuy = corregir_codigos_postales(df_univ_nac_de_jujuy)
    escribir_en_archivo_txt(df_univ_nac_de_jujuy, "UNJ")


def transformar_datos_univ_palermo():
    df_univ_de_palermo = normalizar_universidad("universidad de palermo")
    df_univ_de_palermo = corregir_codigos_postales(df_univ_de_palermo)
    escribir_en_archivo_txt(df_univ_de_palermo, "UP")


def cargar_datos_univ_nac_de_jujuy():
    pass


def cargar_datos_univ_palermo():
    pass
