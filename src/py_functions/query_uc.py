from dataclasses import replace
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
import pandas as pd
from pathlib import Path
import sys

os.chdir(Path(sys.path[0]) / "../..")
print(Path.cwd())

load_dotenv()

db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASS")

uc_query = open("src/sql/query_uc.sql", "r")

engine = create_engine(
    f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
)

df_uc = pd.read_sql_query(uc_query.read(), engine)
uc_query.close()

df_uc.to_csv("files/raw/raw_uc.csv", replace=True)
