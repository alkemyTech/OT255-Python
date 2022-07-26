import os
import shelve
import sys
from pathlib import Path

import pandas as pd
from pandas.api.types import is_string_dtype


# -- FUNCTIONS --
# delete unexpected spaces of dataframe values
def border_blank_deleter(x):
    if x[0].isspace():
        x = x[1:]
    if x[-1].isspace():
        x = x[:-1]
    return x


# -- INITIAL CONFIG --
# set the repository main folder as cwd
os.chdir(Path(sys.path[0]) / "../..")

# set the name of the university as a variable to simplify code reuse.
univ_name = "uba"
# set the origin and destination paths as a variable to simplify code reuse.
raw_path = Path.cwd() / "files/raw"
modified_path = Path.cwd() / "files/modified"

# check if destination folder exists. create them if not.
if not os.path.isdir(Path.cwd() / "files/modified"):
    os.makedirs(Path.cwd() / "files/modified")

# -- SCRIPT --
# load file name of the last file with shelve module.
shelf_file = shelve.open(f"{raw_path}/../temp/last_file")
file_name = shelf_file[f"{univ_name}_filename"]
shelf_file.close()

# load university dataframe into a variable with generic 'univ' name to simplify code reuse.
df_univ = pd.read_csv(raw_path / "".join([file_name, ".csv"]), index_col=False)

# load location/postal code dataframe directly from given url.
url = (
    "https://drive.google.com/file/d/1or8pr7-XRVf5dIbRblSKlRmcP0wiP9QJ/view?usp=sharing"
)
url = "https://drive.google.com/uc?id=" + url.split("/")[-2]
df_location = pd.read_csv(url, index_col=False)
# rename columns to simplify future merging with university dataframe.
rename_Dict = {"codigo_postal": "postal_code", "localidad": "location"}
df_location = df_location.rename(columns=rename_Dict)

# check correspondence between column used for merging.
assert (
    df_location["postal_code"].dtype == df_univ["postal_code"].dtype
), "postal_code should have the same dtype in both data frames"

# merge and rearrange resulting columns.
df_univ = df_univ.merge(df_location, on="postal_code", how="left")
df_univ = df_univ[
    [
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
]

# change columns dtype to meet expected output.
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

df_univ = df_univ.astype(dtype_Dict)

# apply function to delete unexpected spaces and lowercase string columns.
for column in df_univ:
    if is_string_dtype(df_univ[column]):
        df_univ[column] = df_univ[column].map(border_blank_deleter)
        df_univ[column] = df_univ[column].map(str.lower)

# delete hyphens if exist in designed columns.
for column in ["university", "career", "location"]:
    df_univ[column].replace("-", " ", regex=True, inplace=True)

# change the format of the values in gender column.
df_univ["gender"] = df_univ["gender"].map({"f": "female", "m": "male"})

# export processed csv to destination folder.
df_univ.to_csv(modified_path / "".join([file_name, ".csv"]), index=False)
