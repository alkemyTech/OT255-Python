import logging
from pathlib import Path

import pandas as pd
import psycopg2
from decouple import config


def extraer_univ_jfk():
    logging.info("Extrayendo datos de la Universidad J.F. Kennedy")
    conn = psycopg2.connect(
        database=config("DB_NAME"),
        user=config("DB_USER"),
        password=config("DB_PASS"),
        host=config("DB_HOST"),
        port=config("DB_PORT"),
    )

    ruta = Path(__file__).parent.parent.parent / "src" / "sql"

    with open(ruta / "query_universidad_j_f_kennedy.sql", "r") as archivo:
        comando = archivo.read()
    data = pd.read_sql_query(comando, conn)

    ruta_csv = Path.cwd() / "files" / "raw" / "univ_jfk_raw.csv"
    data.to_csv(ruta_csv, index=False)
