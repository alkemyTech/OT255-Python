from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

from env_call import *


def extraccion_PyOp(university):
    """
    Esta funcion implementa el archivo env_call.py que se encarga de leer y definir las variables para la conexion a la base de datos.

    :param university: recibe STR con el nombre del archivo SQL a consultar SIN LA EXTENSION .sql, ya que lo completa automáticamente. Esto puede ser modificado
    modificando la variable "path_sql"
    """
    # Ubicacion de la Query
    path_sql = open("src/sql/" + university + ".sql", "r", encoding="utf-8")
    engine = create_engine(
        "postgresql://{}:{}@{}:{}/{}".format(
            user_db, password_db, host_db, port_db, database_db
        )
    )
    # Lectura de la Query
    df = pd.read_sql_query(path_sql.read(), engine)
    # Exportar a CSV
    df.to_csv(
        Path(__file__).parent.parent.parent / "files" / "raw" / (university + ".csv"),
        index=False,
    )


# extraccion_PyOp("flores")
# extraccion_PyOp("villa_maria")
