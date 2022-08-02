from pathlib import Path

import pandas as pd
import sqlalchemy as db

import db_env


def db_connect():
    """
    Establece conexion con base de datos
    Configura las variables de conexion con el modulo db_env
    Se debe adjuntar archivo .env correspondiente a la base de datos
    """
    engine_conn = db.create_engine(
        f"postgresql://{db_env.db_user}:{db_env.db_pass}@{db_env.db_host}:{db_env.db_port}/{db_env.db_name}"
    )
    return engine_conn


def university_sqlFile_path(university):
    """
    Accede al archivo sql segun corresponda a la universidad indicada como parametro
    El parametro debe ser el nombre de la universidad segun normalizacion acordada (str)
    Devuelve la ruta de acceso al archivo sql
    """
    f_path = Path.cwd() / "src" / "sql"
    file_name = f"{f_path}/{university}.sql"
    return file_name


def db_extract(university):
    """
    Extrae en un archivo .csv los datos de la tabla correspondiente a la universidad enviada como parametro
    El archivo se alojara en la carpeta ./files/raw/db_[nombre_universidad].csv
    El parametro debe ser el nombre de la universidad segun normalizacion acordada (str)
    """
    sql_path = university_sqlFile_path(university)
    csv_path = Path.cwd() / "files" / "raw"
    with open(sql_path, "r") as sql_file:
        df_query = pd.read_sql_query(sql_file.read(), db_connect())
        pd.DataFrame.to_csv(df_query, f"{csv_path}/db_{university}.csv", sep=",")


def normalize_data_univ(university):
    """
    Se normalizan los datos del archivo .csv correspondiente a la universidad enviada como parametro
    El parametro debe ser el nombre de la universidad segun normalizacion acordada (str)
    Devuelve un data frame con los valores normalizados
    """
    f_csv_path = Path.cwd() / "files" / "raw" / ("db_" + university + ".csv")
    print(f_csv_path)
    df_univ = pd.read_csv(
        f_csv_path,
        dtype={
            "university": str,
            "carrer": str,
            "inscription_date": str,
            "first_name": str,
            "last_name": str,
            "gender": str,
            "age": int,
            "postal_code": str,
            "location": str,
            "email": str,
        },
        index_col=False,
    )

    df_univ["university"] = (
        df_univ["university"].str.lower().str.replace("_", " ").str.strip(" -_")
    )
    df_univ["career"] = (
        df_univ["career"].str.lower().str.replace("_", " ").str.strip(" -_")
    )
    df_univ["first_name"] = df_univ["first_name"].str.lower().str.strip(" -_")
    df_univ["last_name"] = df_univ["last_name"].str.lower().str.strip(" -_")
    df_univ["location"] = df_univ["location"].str.lower().str.strip(" -_")
    df_univ["email"] = df_univ["email"].str.lower().str.strip(" -_")
    df_univ["inscription_date"] = pd.to_datetime(df_univ["inscription_date"])
    df_univ["gender"] = df_univ["gender"].replace(
        {"M": "male", "F": "Female"}, regex=True
    )

    return df_univ


# 3FEB
def normalize_data_postal_code():
    """
    Se normalizan los datos de archivo codigos_postales.csv
    El codigo postale es un valor unico. Localidades se repiten ya que pueden corresponder a distintos codigos postales
    Devuelve un data frame con los valores normalizados
    """
    f_csv_path = Path.cwd() / "files" / "raw" / ("codigos_postales.csv")
    df_postal_code = pd.read_csv(
        f_csv_path, dtype={"localidad": str, "codigo_postal": str}, index_col=False
    )
    df_postal_code = df_postal_code.rename(
        columns={"codigo_postal": "postal_code", "localidad": "location"}
    )
    df_postal_code["location"] = df_postal_code["location"].str.lower()

    return df_postal_code


# 3FEB
def postal_code_merge_db(university):
    """
    Unifica datos normalizados de la universidad pasada como parametro con los datos del archivo codigos_postales.csv normalizados
    La union se realiza teniendo en cuenta el valor 'postal_code' de cada tabla
    El parametro debe ser el nombre de la universidad segun normalizacion acordada (str)
    Devuelve un data frame con los valores mergeados
    """
    df_univ = normalize_data_univ(university)
    df_univ = df_univ.drop(["Unnamed: 0", "location"], axis=1)
    df_postal_code = normalize_data_postal_code()
    df_universidad_merged = pd.merge(df_univ, df_postal_code, on="postal_code")

    return df_universidad_merged


# 3FEB
def csvByPostalCode_to_txt(university):
    """
    Convierte los datos normalizados y mergeados segun 'postal_code' de la universidad pasada como parametro
    El parametro debe ser el nombre de la universidad segun normalizacion acordada (str)
    El archivo se alojara en la carpeta ./files/modified/normalized_[nombre_universidad].txt
    """
    postal_code_merge_db(university).to_csv(
        Path.cwd() / "files" / "modified" / ("normalized_" + university + ".txt")
    )


# UTN
def normalize_data_location():
    """
    Se normalizan los datos de archivo codigos_postales.csv
    Localidad es un valor unico. Codigos postales agrupados por localidad. A una localidad pueden corresponder varios codigos postales.
    Devuelve un data frame con los valores normalizados
    """
    f_csv_path = Path.cwd() / "files" / "raw" / ("codigos_postales.csv")
    df_location = pd.read_csv(
        f_csv_path, dtype={"localidad": str, "codigo_postal": str}, index_col=False
    )

    df_location = df_location.rename(
        columns={"codigo_postal": "postal_code", "localidad": "location"}
    )
    df_location["location"] = df_location["location"].str.lower()
    df_location = df_location.groupby("location").postal_code.apply(list).reset_index()

    return df_location


# UTN
def location_merge_db(university):
    """
    Unifica datos normalizados de la universidad pasada como parametro con los datos del archivo codigos_postales.csv normalizados
    La union se realiza teniendo en cuenta el valor 'location' de cada tabla
    El parametro debe ser el nombre de la universidad segun normalizacion acordada (str)
    Devuelve un data frame con los valores mergeados
    """

    df_univ = normalize_data_univ(university)
    df_univ = df_univ.drop(["Unnamed: 0", "postal_code"], axis=1)
    df_location = normalize_data_location()
    df_universidad_merged = pd.merge(df_univ, df_location, on="location")

    return df_universidad_merged


# UTN
def csvByLocation_to_txt(university):
    """
    Convierte los datos normalizados y mergeados segun 'location' de la universidad pasada como parametro
    El parametro debe ser el nombre de la universidad segun normalizacion acordada (str)
    El archivo se alojara en la carpeta ./files/modified/normalized_[nombre_universidad].txt
    """
    location_merge_db(university).to_csv(
        Path.cwd() / "files" / "modified" / ("normalized_" + university + ".txt")
    )

db_extract('3feb')
csvByPostalCode_to_txt('3feb')
db_extract('utn')
csvByLocation_to_txt('utn')