"""Python function for pandas processing for universidad del salvador"""
import pandas as pd
from dotenv import load_dotenv

# Loading variables from dotenv
load_dotenv()

destFolder = os.getenv('destFolder')
postal_code = os.getenv('postal_code')

def pandas_processing_salvador():
    """Python function for pandas processing for universidad del salvador"""
    with open(f'{destFolder}/universidad_del_salvador.csv', 'r') as f:
        df = pd.read_csv(f, encoding='latin-1')
        df = df.astype({'age': 'int'})
        df = df.drop('postal_code', axis=1)
        df['university'] = df['university'].str.replace('_', ' ').str.lower().str.strip()
        df['career'] = df['career'].str.replace('_', ' ').str.lower().str.strip()
        df['first_name'] = df['first_name'].str.replace('_', ' ').str.lower()
        df['last_name'] = df['last_name'].str.replace('_', ' ').str.lower()
        df.loc[df['gender'] == 'M', 'gender'] = 'male'
        df.loc[df['gender'] == 'F', 'gender'] = 'female'
        df['location'] = df['location'].str.lower()
        df['email'] = df['email'].str.replace('_', ' ').str.lower()

    with open(f'{postal_code}/codigos_postales.csv', 'r') as cp:
        dfpostal_code = pd.read_csv(cp, encoding='latin-1')
        dfpostal_code.rename(columns={'codigo_postal': 'postal_code', 'localidad': 'location'}, inplace=True)
        dfpostal_code = dfpostal_code.astype({'postal_code': 'str'})
        dfpostal_code['location'] = dfpostal_code['location'].str.lower()

    df = df.merge(dfpostal_code, on='location', how='left')
    df.to_csv(r'universidad_del_salvador.txt', header=None, index=None, sep=' ', mode='a')
