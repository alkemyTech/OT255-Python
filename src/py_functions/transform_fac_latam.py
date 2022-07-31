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
        / "g255_fac_latam.csv"
    )
    # Guardo la tabla modificada
    fac_latam.to_csv(ruta_csv, index=False)
