import logging
from pathlib import Path

import pandas as pd


def transform_univ_jfk():
    """Transforma la tabla raw de la universidad J.F. Kennedy de ciencias
    sociales en el formato indicado"""
    logging.info("Transformando los datos de la Universidad J.F. Kennedy")
    # Ruta de donde leere el archivo csv
    ruta_univ_jfk = Path.cwd() / "files" / "raw" / "univ_jfk_raw.csv"
    # Ruta donde está el archivo que usaré para location
    ruta_cp = Path.cwd() / "files" / "raw" / "codigos_postales.csv"
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
    ruta_csv = Path.cwd() / "files" / "modified" / "g255_univ_jfk_.csv"
    # Guardo el .csv
    univ_jfk.to_csv(ruta_csv, index=False)
