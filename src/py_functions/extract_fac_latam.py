import logging
from pathlib import Path

import pandas as pd
import psycopg2
from decouple import config


def extraer_latinoam():
    logging.info(
        "Extrayendo datos de la Facultad Latinoamericana de Ciencias Sociales"
    )
    conn = psycopg2.connect(
        database=config("DB_NAME"),
        user=config("DB_USER"),
        password=config("DB_PASS"),
        host=config("DB_HOST"),
        port=config("DB_PORT"),
    )

    ruta = Path(__file__).parent.parent.parent / "src" / "sql"

    with open(
        ruta / "query_facultad_latinoamericana_de_ciencias_sociales.sql", "r"
    ) as archivo:
        comando = archivo.read()
    data = pd.read_sql_query(comando, conn)

    ruta_csv = Path.cwd() / "files" / "raw" / "fac_latam_raw.csv"
    data.to_csv(ruta_csv, index=False)
