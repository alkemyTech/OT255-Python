import pandas as pd

def cleaningData():
    inter = pd.read_csv('/home/lucasfersantos/Documentos/OT255-Python/files/raw/universidad_interamericana.csv')
    pampa = pd.read_csv('/home/lucasfersantos/Documentos/OT255-Python/files/raw/universidad_lapampa.csv')
    cod_postal = pd.read_csv('/home/lucasfersantos/Documentos/OT255-Python/files/raw/codigos_postales.csv')

    cod_postal = cod_postal.rename(columns={'codigo_postal': 'postal_code', 'localidad': 'location'})
    cod_postal['postal_code'] = cod_postal['postal_code'].astype('str')
    cod_postal['location'] = cod_postal['location'].str.lower().str.replace("-"," ").str.strip()

    inter['university'] = inter['university'].str.lower().str.replace("-"," ").str.strip()
    inter['carrer'] = inter['carrer'].str.lower().str.replace("-"," ").str.strip()
    inter['first_name'] = inter['first_name'].str.lower()
    inter['email'] = inter['email'].str.lower()
    inter['gender'] = inter['gender'].replace('M', 'male').replace('F', 'female')
    inter['inscription_date'] = pd.to_datetime(inter['inscription_date'], format='%Y-%m-%d')
    inter['location'] = inter['location'].str.lower()

    dfinter = pd.merge(inter, cod_postal, on='location')
    dfinter = dfinter.drop(['Unnamed: 0'], axis=1)

    cols = list(dfinter.columns)
    dfinter = dfinter[cols[0:7]+[cols[-1]]+cols[7:9]]
    dfinter.to_csv('/home/lucasfersantos/Documentos/OT255-Python//files/modified/universidad_interamericana.txt')

    pampa['university'] = pampa['university'].str.lower()
    pampa['carrer'] = pampa['carrer'].str.lower()
    pampa['inscription_date'] = pd.to_datetime(pampa['inscription_date'], format='%Y-%m-%d')
    pampa['first_name'] = pampa['first_name'].str.lower()
    pampa['gender'] = pampa['gender'].replace('M', 'male').replace('F', 'female')
    pampa['postal_code'] = pampa['postal_code'].astype('str')

    dfpampa = pd.merge(pampa, cod_postal, on='postal_code')
    cols2 = list(dfpampa.columns)
    dfpampa = dfpampa.drop(['Unnamed: 0'], axis=1)
    dfpampa = dfpampa[cols2[1:9]+[cols2[-1]]+cols2[9:10]]
    dfpampa.to_csv('/home/lucasfersantos/Documentos/OT255-Python//files/modified/universidad_lapampa.txt')

cleaningData()