import os
import pandas as pd
import numpy as np
import click

from tqdm import tqdm
from unicodedata import normalize

from pjud import data

def create_comunas(path = "data/raw/subdere"):
    tqdm.pandas()

    df_provincias = pd.read_excel(f"{path}/provinciasChile.xls")

    # Transformo a mayusculas las columnas de mi interes
    df_provincias['Nombre Región'] = df_provincias['Nombre Región'].str.upper()
    df_provincias['Nombre Provincia'] = df_provincias['Nombre Provincia'].str.upper()
    df_provincias['Nombre Comuna'] = df_provincias['Nombre Comuna'].str.upper()

    click.echo('Eliminando Tildes')
    cols = df_provincias.select_dtypes(include = ["object"]).columns
    df_provincias[cols] = df_provincias[cols].progress_apply(data.cleandata.elimina_tilde)

    # Acá se debe analiza las comunas que presentar diferentes nombres en los dataset que se estan trabajando, 
    # por lo que se procede a unificar la info de comunas con la existente en el Codigo Organico de Tribunales.

    # Cambio nombre provincia para coincidir con otro df

    df_provincias.loc[df_provincias['Nombre Provincia'] == 'ANTARTICA CHILENA', 'Nombre Provincia'] = 'LA ANTARTICA CHILENA'
    df_provincias.loc[df_provincias['Nombre Comuna'] == 'COIHAYQUE', 'Nombre Comuna'] = 'COIHAIQUE'
    df_provincias.loc[df_provincias['Nombre Comuna'] == 'PAIGUANO', 'Nombre Comuna'] = 'PAIHUANO'
    df_provincias.loc[df_provincias['Nombre Comuna'] == 'TILTIL', 'Nombre Comuna'] = 'TIL TIL'
    df_provincias.loc[df_provincias['Nombre Comuna'] == 'EL OLIVAR', 'Nombre Comuna'] = 'OLIVAR'

    data.save_feather(df_provincias, 'generates_Provincias', path='./data/interim/subdere')
    click.echo('Generado archivo Feather. Proceso Terminado')

def load_data_censo(path = "data/raw/censo"):
    tqdm.pandas()

    # Analizo contra los datos extraidos en COT

    df_censo = pd.read_excel(f"{path}/1_1_POBLACION.xls", sheet_name = "Comuna")

    df_censo.drop(['Unnamed: 0'], axis='columns', inplace=True)
    df_censo.drop(0, axis='rows', inplace=True)

    old_columns = []
    for col in range(1,18):
        old_columns.append(f"Unnamed: {col}")

    new_columns = []
    for col in old_columns:
        new_columns.append(df_censo[col][1])

    columnas = dict(zip(old_columns, new_columns))

    df_censo.rename(columns=columnas, inplace=True)

    click.echo('Eliminando tildes')
    cols = df_censo.select_dtypes(include = ["object"]).columns
    df_censo[cols] = df_censo[cols].progress_apply(data.cleandata.elimina_tilde)

    seleccion_censo_comunas = df_censo.loc[df_censo["EDAD"].str.contains("Total")]
    seleccion_censo_comunas.drop(2, axis='rows', inplace=True)

    seleccion_censo_comunas.loc[seleccion_censo_comunas['NOMBRE COMUNA'] == 'COYHAIQUE', 'NOMBRE COMUNA'] = 'COIHAIQUE'
    seleccion_censo_comunas.loc[seleccion_censo_comunas['NOMBRE COMUNA'] == 'PAIGUANO', 'NOMBRE COMUNA'] = 'PAIHUANO'
    seleccion_censo_comunas.loc[seleccion_censo_comunas['NOMBRE COMUNA'] == 'TILTIL', 'NOMBRE COMUNA'] = 'TIL TIL'
    seleccion_censo_comunas.loc[seleccion_censo_comunas['NOMBRE COMUNA'] == 'AYSEN', 'NOMBRE COMUNA'] = 'AISEN'

    data.save_feather(seleccion_censo_comunas, 'generates_Censo2017', path='./data/processed/censo')
    click.echo('Generado archivo Feather. Proceso Terminado')



