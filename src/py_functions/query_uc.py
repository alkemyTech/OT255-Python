import datetime
import os
import shelve
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

os.chdir(Path(sys.path[0]) / "../..")

if not os.path.isdir(Path.cwd() / "files/raw"):
    os.makedirs(Path.cwd() / "files/raw")
if not os.path.isdir(Path.cwd() / "files/modified"):
    os.makedirs(Path.cwd() / "files/modified")
if not os.path.isdir(Path.cwd() / "files/temp"):
    os.makedirs(Path.cwd() / "files/temp")

shelf_file = shelve.open("files/temp/last_file")
univ_name = "uc"

load_dotenv()

db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASS")

query = open(f"src/sql/query_{univ_name}.sql", "r")

engine = create_engine(
    f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
)

df_query = pd.read_sql_query(query.read(), engine)

today = datetime.datetime.now()

file_name = "_".join(
    ["-".join([str(today.year), str(today.month).zfill(2), str(today.day).zfill(2)]), univ_name]
)

df_query.to_csv(f"files/raw/{file_name}.csv", index=False)
