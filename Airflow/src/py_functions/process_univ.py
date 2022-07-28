import os
import shelve
import sys
from pathlib import Path

import pandas as pd
from pandas.api.types import is_string_dtype


def main(univ: str):
    assert isinstance(univ, str), "University acronym must be string type."

    # -- FUNCTIONS --
    # change column dtype for columns present in the current dataframe
    def column_dtype_changer(df):
        dtype_dict = {
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

        for column in df:
            df[column] = df[column].apply(dtype_dict[column])
        return df

    # change format for columns with str dtype in the current dataframe
    def string_column_formatter(df):
        for column in df:
            if is_string_dtype(df[column]):
                # delete hyphens except in 'inscription_date' column
                if column != "inscription_date":
                    df[column].replace("-", " ", regex=True, inplace=True)
                df[column] = df[column].map(border_blank_deleter)
                df[column] = df[column].map(str.lower)
        return df

    # delete unexpected spaces for current dataframe values
    def border_blank_deleter(x):
        if x[0].isspace():
            x = x[1:]
        if x[-1].isspace():
            x = x[:-1]
        return x

    # -- INITIAL CONFIG --
    # set the repository main folder as cwd
    os.chdir(Path(sys.path[0]) / "..")

    # set the name of the university as a variable to simplify code reuse.
    univ_name = univ
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

    # - prepare university dataframe -
    # load university dataframe into a variable with generic 'univ' name to simplify code reuse.
    df_univ = pd.read_csv(raw_path / "".join([file_name, ".csv"]), index_col=False)
    # change columns dtype to match expected output.
    column_dtype_changer(df_univ)
    # format columns with string dtype to match expected output.
    string_column_formatter(df_univ)
    # change the format of the values in gender column.
    df_univ["gender"] = df_univ["gender"].map({"f": "female", "m": "male"})

    # - prepare location dataframe -
    # load location/postal code dataframe directly from given url.
    url = "https://drive.google.com/file/d/1or8pr7-XRVf5dIbRblSKlRmcP0wiP9QJ/view?usp=sharing"
    url = "https://drive.google.com/uc?id=" + url.split("/")[-2]
    df_location = pd.read_csv(url, index_col=False)
    # rename columns to simplify future merging with university dataframe.
    rename_dict = {"codigo_postal": "postal_code", "localidad": "location"}
    df_location = df_location.rename(columns=rename_dict)
    # change columns dtype to match expected output.
    column_dtype_changer(df_location)
    # format columns with string dtype to match expected output.
    string_column_formatter(df_location)

    # - merge both dataframes -
    if "location" in df_univ.columns:
        # check correspondence between column used for merging.
        assert (
            df_location["location"].dtype == df_univ["location"].dtype
        ), "location column should have the same dtype in both data frames"

        # generate an array to keep every possible postal code for duplicated location names.
        df_location = (
            df_location.groupby("location").postal_code.apply(list).reset_index()
        )
        # merge both dataframes on 'location' column.
        df_univ = df_univ.merge(df_location, on="location", how="left")

    elif "postal_code" in df_univ.columns:
        # check correspondence between column used for merging.
        assert (
            df_location["postal_code"].dtype == df_univ["postal_code"].dtype
        ), "postal_code column should have the same dtype in both data frames"

        # merge both dataframes on 'postal_code' column.
        df_univ = df_univ.merge(df_location, on="postal_code", how="left")

    # rearrange resulting columns to match expected output.
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

    # - export resulting dataframe -
    df_univ.to_csv(modified_path / "".join([file_name, ".csv"]), index=False)


if __name__ == "__main__":
    main()
