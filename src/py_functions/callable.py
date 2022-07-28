from pathlib import Path

import pandas as pd
import sqlalchemy as db

import db_env


def db_connect():
    engine_conn = db.create_engine(
        f"postgresql://{db_env.db_user}:{db_env.db_pass}@{db_env.db_host}:{db_env.db_port}/{db_env.db_name}"
    )
    return engine_conn


def university_sqlFile_path(university):
    f_path = Path.cwd() / "src" / "sql"
    file_name = f"{f_path}/{university}.sql"
    return file_name


def db_extract(university):
    sql_path = university_sqlFile_path(university)
    csv_path = Path.cwd() / "files" / "raw"
    with open(sql_path, "r") as sql_file:
        df_query = pd.read_sql_query(sql_file.read(), db_connect())
        pd.DataFrame.to_csv(df_query, f"{csv_path}/db_{university}.csv", sep=",")
