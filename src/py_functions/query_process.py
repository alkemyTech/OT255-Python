def db_extract_dag(university):
    from pathlib import Path

    import pandas as pd
    import sqlalchemy as db

    db_extract(university)


def db_connect():
    """
    Establece conexion con base de datos
    Configura las variables de conexion con el modulo db_env
    Se debe adjuntar archivo .env correspondiente a la base de datos
    """
    import os

    import sqlalchemy as db
    from dotenv import load_dotenv

    load_dotenv()

    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    engine_conn = db.create_engine(
        # f"postgresql://{db_env.db_user}:{db_env.db_pass}@{db_env.db_host}:{db_env.db_port}/{db_env.db_name}"
        f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    )
    return engine_conn


def university_sqlFile_path(university):
    """
    Accede al archivo sql segun corresponda a la universidad indicada como parametro
    El parametro debe ser el nombre de la universidad segun normalizacion acordada (str)
    Devuelve la ruta de acceso al archivo sql
    """
    from pathlib import Path

    import pandas as pd
    import sqlalchemy as db

    f_path = Path.cwd() / "src" / "sql"
    file_name = f"{f_path}/{university}.sql"
    return file_name


def db_extract(university):
    """
    Extrae en un archivo .csv los datos de la tabla correspondiente a la universidad enviada como parametro
    El archivo se alojara en la carpeta ./files/raw/db_[nombre_universidad].csv
    El parametro debe ser el nombre de la universidad segun normalizacion acordada (str)
    """
    from pathlib import Path

    import pandas as pd
    import sqlalchemy as db

    sql_path = university_sqlFile_path(university)
    csv_path = Path.cwd() / "files" / "raw"
    with open(sql_path, "r") as sql_file:
        df_query = pd.read_sql_query(sql_file.read(), db_connect())
        pd.DataFrame.to_csv(df_query, f"{csv_path}/db_{university}.csv", sep=",")
