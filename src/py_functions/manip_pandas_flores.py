from pathlib import Path

import pandas as pd


def manip_pandas_flores():

    university = "flores"
    # Path de los csv
    path_univ_csv = (
        Path(__file__).parent.parent.parent / "files" / "raw" / (university + ".csv")
    )
    path_location = (
        Path(__file__).parent.parent.parent / "files" / "raw" / "codigos_postales.csv"
    )
    # Lectura de los CSV y conversion de tipos
    df_univ = pd.read_csv(
        path_univ_csv,
        dtype={
            "university": str,
            "career": str,
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
    df_location = pd.read_csv(
        path_location, dtype={"localidad": str, "codigo_postal": str}, index_col=False
    )
    # Cambio el nombre de las columnas de df_location
    df_location.rename(
        columns={"codigo_postal": "postal_code", "localidad": "location"}, inplace=True
    )

    # Aseguro que no hayan ducplicados en las localidades
    df_location = df_location.groupby("location").postal_code.apply(list).reset_index()
    # Joineo df_location y df_univ
    df_univ = df_univ.merge(df_location, on="location", how="left")
    # Borro columna que se genera y cambio el nombre de otra.
    df_univ = df_univ.drop("postal_code_y", axis=1)
    df_univ = df_univ.rename(columns={"postal_code_x": "postal_code"})

    # Reemplazo "_" por " "
    df_univ = df_univ.replace(to_replace="_", value=" ", regex=True)

    # Manipulacion de las columnas
    # Elimino espacios, guiones innecesarios y convierto a minusculas

    df_univ["university"] = df_univ["university"].str.lower()
    df_univ["university"] = df_univ["university"].str.strip(" -")

    df_univ["career"] = df_univ["career"].str.lower()
    df_univ["career"] = df_univ["career"].str.strip(" -")

    df_univ["first_name"] = df_univ["first_name"].str.lower()
    df_univ["first_name"] = df_univ["first_name"].str.strip(" -")

    df_univ["last_name"] = df_univ["last_name"].str.lower()
    df_univ["last_name"] = df_univ["last_name"].str.strip(" -")

    df_univ["location"] = df_univ["location"].str.lower()
    df_univ["location"] = df_univ["location"].str.strip(" -")

    df_univ["email"] = df_univ["email"].str.lower()
    df_univ["email"] = df_univ["email"].str.strip(" -")

    # Modificacion columna "gender"
    df_univ["gender"] = df_univ["gender"].replace(to_replace="M", value="male")
    df_univ["gender"] = df_univ["gender"].replace(to_replace="F", value="female")

    # Exporto a .txt
    df_univ.to_csv(
        Path(__file__).parent.parent.parent
        / "files"
        / "modified"
        / ("g255_" + university + ".txt")
    )
    # return df_univ


# manip_pandas_flores()
