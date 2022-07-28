"""Python script for quering DataBase"""
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Loading variables from dotenv
load_dotenv()

postgres_conn_id = os.getenv('postgres_conn_id')
path_c = os.getenv('path_c')
path_s = os.getenv('path_s')
destFolder = os.getenv('destFolder')
file_list=[path_c, path_s]

def queryDatabase(file):
    """Python function to extract queries and query database"""
    engine = create_engine(postgres_conn_id)
    query = open(file, 'r')
    result = pd.read_sql(query.read(), engine)
    fileName = file.split('.')[0]
    result.to_csv(f'{destFolder}{fileName}.csv', index=False)

for i in file_list:
    queryDatabase(i)
