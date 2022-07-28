"""DAG created for processing data from postgres using pandas and loading it to Amazon S3"""
import airflow
import logging
import requests
import csv
import shutil
from datetime import datetime
from airflow.operators import python_operator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from requests.structures import CaseInsensitiveDict
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

# Logger configuration
logging.basicConfig(
    level=logging.INFO,
    format='%Y-%m-%d - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler('univ_b.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

postgres_conn_id = Variable.get('postgres_conn_id')
s3_bucket = Variable.get('amazon_bucket')
s3_key = Variable.get('amazon_key')
url_s = Variable.get('salvador_url')
url_c = Variable.get('comahue_url')
auth = Variable.get('authorization')
destFolder = Variable.get('destination_folder')



def queryDatabase():
    """Python function to extract queries and query database"""
    urls = [url_s, url_c]
    headers = CaseInsensitiveDict()
    headers["Authorization"] = auth
    engine = create_engine(postgres_conn_id)

    for i in urls:
        r = requests.get(i, allow_redirects=True, headers=headers)
        db = scoped_session(sessionmaker(bind=engine))
        res = db.execute(r.text).fetchall()
        db.commit()
        db.close()
        x = (i.split('/')[-1]).split('.')
        file = f'{x[0]}.csv'
        with open(file, 'w', newline='') as csvfile:
            outcsv = csv.writer(csvfile)
            header = ['university',
                      'career',
                      'insription_date',
                      'first_name',
                      'last_name',
                      'gender',
                      'age',
                      'postal_code',
                      'location',
                      'email']
            outcsv.writerow(header)
            for record in res:
                outcsv.writerow(record)
            shutil.move(file, f'{destFolder}/{file}')


def pandas_processing_comahue():
    """Python function for pandas processing for universidad nacional del comahue"""
    with open(f'{destFolder}/universidad_del_comahue.csv', 'r') as f:
        df = pd.read_csv(f, encoding='latin-1')
        df = df.assign(university='univ. nacional del comahue')
        df = df.astype({'age': 'int'})
        df = df.astype({'postal_code': 'str'})
        # df = df.drop('location', axis=1)
        for i in df.index:
            df.at[i, 'career'] = (df.at[i, 'career']).lower()
            df.at[i, 'first_name'] = (df.at[i, 'first_name']).lower()
            df.at[i, 'last_name'] = (df.at[i, 'last_name']).lower()
            df.at[i, 'email'] = (df.at[i, 'email']).lower()
            if df.at[i, 'gender'] == 'M':
                (df.at[i, 'gender']) = 'male'
            elif df.at[i, 'gender'] == 'F':
                (df.at[i, 'gender']) = 'female'
    with open('codigos_postales.csv', 'r') as cp:
        dfpostal_code = pd.read_csv(cp, encoding='latin-1')
        dfpostal_code.rename(columns={'codigo_postal': 'postal_code', 'localidad': 'location'}, inplace=True)
        dfpostal_code = dfpostal_code.astype({'postal_code': 'str'})
        for i in dfpostal_code.index:
            dfpostal_code.at[i, 'location'] = (dfpostal_code.at[i, 'location']).lower()

    df = df.merge(dfpostal_code, on='postal_code', how='left')
    df.to_csv(rf'{destFolder}/universidad_del_comahue.txt', header=None, index=None, sep=' ', mode='a')

def pandas_processing_salvador():
    """Python function for pandas processing for universidad del salvador"""
    with open(f'{destFolder}/universidad_del_salvador.csv', 'r') as f:
        df = pd.read_csv(f,encoding='latin-1')
        df = df.assign(university='universidad del salvador')
        df = df.astype({'age':'int'})
        #df = df.drop('postal_code', axis=1)
        for i in df.index:
            df.at[i, 'career'] = (df.at[i, 'career']).lower().replace('_',' ').strip()
            df.at[i, 'first_name'] = (df.at[i, 'first_name']).lower().replace('_',' ').strip()
            df.at[i, 'last_name'] = (df.at[i, 'last_name']).lower().replace('_',' ').strip()
            df.at[i, 'email'] = (df.at[i, 'email']).lower().replace('_',' ').strip()
            df.at[i, 'location'] = (df.at[i, 'location']).lower().replace('_',' ').strip()
            if df.at[i, 'gender'] == 'M':
                (df.at[i, 'gender']) = 'male'
            elif df.at[i, 'gender'] == 'F':
                (df.at[i, 'gender']) = 'female'
    with open('codigos_postales.csv','r') as cp:
        dfpostal_code = pd.read_csv(cp, encoding='latin-1')
        dfpostal_code.rename(columns={'codigo_postal': 'postal_code', 'localidad': 'location'},inplace=True)
        dfpostal_code= dfpostal_code.astype({'postal_code':'str'})
        for i in dfpostal_code.index:
            dfpostal_code.at [i,'location']= (dfpostal_code.at [i,'location']).lower()

    df = df.merge(dfpostal_code, on='location', how='left')
    df.to_csv(rf'{destFolder}/universidad_del_salvador.txt', header=None, index=None, sep=' ', mode='a')


logging.info('DAG para univ del Salvador y Comahue')
with airflow.DAG(
        'dag_univ_salvador_comahue',
        description='DAG para univ del Salvador y Comahue',
        schedule_interval='@hourly',
        start_date=datetime(2022, 7, 17)
) as dag:

    logging.info('Querying database...')
    # Here we extract sql archives for further use
    extract_sql = python_operator.PythonOperator(
        task_id='extract_sql',
        python_callable=queryDatabase,
        retries=5
    )

    logging.info('Processing with pandas...')
    # Here we perform the queries
    with TaskGroup(group_id='queries') as pandas_processing:
        pandas_processing_comahue = python_operator.PythonOperator(
            task_id='pandas_comahue',
            python_callable=pandas_processing_comahue()
        )

        pandas_processing_salvador = python_operator.PythonOperator(
            task_id='pandas_salvador',
            python_callable=pandas_processing_salvador()
        )

        pandas_processing_comahue >> pandas_processing_salvador

    logging.info('Storing results...')
    # Here we store our processed data in amazon aws
    aws_bucket = S3CreateObjectOperator(
        task_id='s3_create_object',
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        replace=True,
        retries = 5
    )

    extract_sql >> pandas_processing >> aws_bucket
