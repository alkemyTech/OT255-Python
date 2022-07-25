import os
import sys
from pathlib import Path

import pandas as pd
from pandas.api.types import is_string_dtype


def border_blank_deleter(x):
    if x[0].isspace():
        x = x[1:]
    if x[-1].isspace():
        x = x[:-1]
    return x


os.chdir(sys.path[0])

current_univ = "uba"

df_uba = pd.read_csv("./data/univ_buenos_aires_2022072114.csv")

assert list(df_uba) == [
    "university",
    "career",
    "inscription_date",
    "first_name",
    "last_name",
    "gender",
    "age",
    "postal_code",
    "location",
    "email",
]

dtype_Dict = {
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
}

df_uba = df_uba.astype(dtype_Dict)

for column in ["university", "career", "location"]:
    df_uba[column].replace("-", " ", regex=True, inplace=True)

for column in df_uba:
    if is_string_dtype(df_uba[column]):
        df_uba[column] = df_uba[column].map(border_blank_deleter)
        df_uba[column] = df_uba[column].map(str.lower)

df_uba["gender"] = df_uba["gender"].map({"f": "female", "m": "male"})
