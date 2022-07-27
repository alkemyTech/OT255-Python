import datetime
import os
import shelve
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


def main(univ: str):
    assert isinstance(univ, str), "University acronym must be string type."

    # -- INITIAL CONFIG --
    # set the repository main folder as cwd
    os.chdir(Path(sys.path[0]) / "../..")

    # set the name of the university as a variable to simplify code reuse.
    univ_name = univ
    # set the destination path as a variable to simplify code reuse.
    raw_path = Path.cwd() / "files/raw"

    # check if destination and temp folders exists. create them if not.
    if not os.path.isdir(raw_path):
        os.makedirs(raw_path)
    if not os.path.isdir(raw_path / "../temp"):
        os.makedirs(raw_path / "../temp")

    # -- ENVIRONMENT VARIABLES --
    # load environment variables with python-dotenv module.
    load_dotenv()

    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")

    # -- SCRIPT --
    # read the sql script into a local variable.
    query = open(f"src/sql/query_{univ_name}.sql", "r")
    # create engine to make the connection with the database.
    engine = create_engine(
        f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    )
    # read query result into a pandas dataframe.
    df_query = pd.read_sql_query(query.read(), engine)
    # incorporate date info to the file name.
    today = datetime.datetime.now()
    file_name = "_".join(
        [
            univ_name,
            "-".join(
                [str(today.year), str(today.month).zfill(2), str(today.day).zfill(2)]
            ),
        ]
    )
    # write query result into a csv file inside the destination folder.
    df_query.to_csv(raw_path / "".join([file_name, ".csv"]), index=False)
    # save file name of the last file with shelve module.
    shelf_file = shelve.open(f"{raw_path}/../temp/last_file")
    shelf_file[f"{univ_name}_filename"] = file_name
    shelf_file.close()


if __name__ == "__main__":
    main()
