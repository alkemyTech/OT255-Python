import logging
from pathlib import Path

import pandas as pd


def transform_fac_latam():
    """Transforma la tabla raw de la facultad latinoamericana de ciencias
    sociales en el formato indicado"""
    logging.info(
        "Transformando los datos de la Facultad Latinoamericana de ciencias"
    )
    # Ruta de donde se va a extraer el archivo raw
    ruta_fac_latam = (
        Path(__file__).parent.parent.parent
        / "files"
        / "raw"
        / "fac_latam_raw.csv"
    )
    # Ruta de donde se va a extraer el archivo que se usará para los codigos
    # postales
    ruta_cp = (
        Path(__file__).parent.parent.parent
        / "files"
        / "raw"
        / "codigos_postales.csv"
    )
    # Leo el csv con panda y se modifica columna por columna
    fac_latam = pd.read_csv(ruta_fac_latam)
    fac_latam["university"] = fac_latam["university"].str.lower()
    fac_latam["university"] = fac_latam["university"].replace(
        {"-": " "}, regex=True
    )
    fac_latam["university"] = fac_latam["university"].replace(
        {" f": "f"}, regex=True
    )

    fac_latam["career"] = fac_latam["career"].str.lower()
    fac_latam["career"] = fac_latam["career"].replace({"-": " "}, regex=True)

    fac_latam["inscription_date"] = pd.to_datetime(
        fac_latam["inscription_date"]
    )
    fac_latam["inscription_date"] = fac_latam["inscription_date"].dt.strftime(
        "%Y-%m-%d"
    )

    fac_latam["first_name"] = fac_latam["first_name"].str.lower()

    fac_latam["last_name"] = fac_latam["last_name"].str.lower()

    fac_latam["gender"] = fac_latam["gender"].replace(
        {"M": "male", "F": "female"}, regex=True
    )

    fac_latam["age"] = fac_latam["age"].astype(int)
    # Leo el archivo que usare para los codigos postales y los transformo para
    # poder hacer el merge
    df_cp = pd.read_csv(ruta_cp)
    df_cp["localidad"] = df_cp["localidad"].str.lower()
    df_cp["codigo_postal"] = df_cp["codigo_postal"].astype(str)
    df_cp = df_cp.groupby("localidad").codigo_postal.apply(list).reset_index()

    fac_latam["location"] = fac_latam["location"].str.lower()
    fac_latam["location"] = fac_latam["location"].replace(
        {"-": " "}, regex=True
    )
    # Hago el merge
    fac_latam = fac_latam.merge(
        df_cp, left_on="location", right_on="localidad", how="left"
    )
    # Cambio la columna "postal_code"
    fac_latam["postal_code"] = fac_latam["codigo_postal"]

    fac_latam["email"] = fac_latam["email"].str.lower()
    fac_latam["email"] = fac_latam["email"].replace({"-": ""}, regex=True)
    # Elimino las columnas que ya no usaré
    fac_latam = fac_latam.drop(["codigo_postal", "localidad"], axis=1)
    # Defino la ruta donde guardaré la tabla modificada
    ruta_csv = (
        Path(__file__).parent.parent.parent
        / "files"
        / "modified"
        / "fac_latam_modified.csv"
    )
    # Guardo la tabla modificada
    fac_latam.to_csv(ruta_csv, index=False)


def transform_univ_jfk():
    """Transforma la tabla raw de la universidad J.F. Kennedy de ciencias
    sociales en el formato indicado"""
    logging.info("Transformando los datos de la Universidad J.F. Kennedy")
    # Ruta de donde leere el archivo csv
    ruta_univ_jfk = (
        Path(__file__).parent.parent.parent
        / "files"
        / "raw"
        / "univ_jfk_raw.csv"
    )
    # Ruta donde está el archivo que usaré para location
    ruta_cp = (
        Path(__file__).parent.parent.parent
        / "files"
        / "raw"
        / "codigos_postales.csv"
    )
    # Leo el archivo con pandas y empiezo a modificar
    univ_jfk = pd.read_csv(ruta_univ_jfk)
    univ_jfk["university"] = univ_jfk["university"].replace(
        {"-": " "}, regex=True
    )

    univ_jfk["career"] = univ_jfk["career"].replace({"-": " "}, regex=True)

    univ_jfk["gender"] = univ_jfk["gender"].replace(
        {"m": "male", "f": "female"}, regex=True
    )

    univ_jfk["age"] = univ_jfk["age"].astype(int)

    univ_jfk["postal_code"] = univ_jfk["postal_code"].astype(str)
    # Leo con pandas el archivo que usaré para location
    df_cp = pd.read_csv(ruta_cp)
    df_cp["localidad"] = df_cp["localidad"].str.lower()
    df_cp["codigo_postal"] = df_cp["codigo_postal"].astype(str)
    # Hago el merge coincidiendo los codigos postales
    univ_jfk = univ_jfk.merge(
        df_cp, left_on="postal_code", right_on="codigo_postal", how="left"
    )
    # Sustituyo la columna location en el dataframe
    univ_jfk["location"] = univ_jfk["localidad"]

    univ_jfk["email"] = univ_jfk["email"].replace({"-": ""}, regex=True)
    # Elimino las columnas que no usaré
    univ_jfk = univ_jfk.drop(["codigo_postal", "localidad"], axis=1)
    # Defino la ruta donde guardaré el .csv modificado
    ruta_csv = (
        Path(__file__).parent.parent.parent
        / "files"
        / "modified"
        / "univ_jfk_modified.csv"
    )
    # Guardo el .csv
    univ_jfk.to_csv(ruta_csv, index=False)


def transform_faclatam_ujfk():
    """Esta funcion ejecuta la funcion transform_fac_latam() y transform_univ_jfk
    para que el python callable solo ejecute esta funcion, y está ejecute las
    demas"""
    transform_fac_latam()
    transform_univ_jfk()
