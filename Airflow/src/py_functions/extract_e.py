from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import os
from pathlib import Path

def uInter():
    pg_hook = PostgresHook(postgre_conn_id='postgre_sql_univ')
    connection = pg_hook.get_conn()

    with open ('src/sql/universidad_interamericana.sql', 'r') as myfile: 
        dataConsulta = pd.read_sql_query(myfile.read(), connection)
        os.makedirs('files', exist_ok=True)
        dataConsulta.to_csv('/files/raw/universidad_interamericana.csv')


def uPampa():
    pg_hook = PostgresHook(postgre_conn_id='postgre_sql_univ')
    connection = pg_hook.get_conn()

    with open ('src/sql/universidad_lapampa.sql', 'r') as myfile: 
        dataConsulta = pd.read_sql_query(myfile.read(), connection)
        os.makedirs('files', exist_ok=True)
        dataConsulta.to_csv('/files/raw/universidad_lapampa.csv')