import pandas as pd
from datetime import datetime
from pjud import data
import os
from tqdm.auto import tqdm
import numpy as np
import click

def convierte_fecha(fecha): 
    try:
        day,month,year = map(int,fecha.split(sep = "-"))
    except:
        #print(f"no pude ejecutar {fecha}")
        return pd.NaT
    
    return datetime(year,month,day) 

def elimina_tilde(str_variable):
    replacements = {'Á': 'A',
                    'É': 'E',
                    'Í': 'I',
                    'Ó': 'O',
                    'Ú': 'U',
                    'Ü': 'U',
                   }

    for a, b in replacements.items():
        str_variable = str_variable.astype(str).str.replace(a, b)
    return str_variable


def elimina_espacios(col):
       if col.dtypes == object:
           return (col.astype(str).str.rstrip())
       return col


def limpia_rit(str_rit):
    return str_rit.replace('--','-')


def limpieza_caracteres(str_col):
    replacements = {'-': '',
                    '\xa0': '',
                    '\n': ''
                   }

    for a, b in replacements.items():
        str_col = str_col.astype(str).str.replace(a, b)
    return str_col

def transforma_numero(str_numero):
    replacements = {"ún": "1",
                    "un": "1",
                    "dós": "2",
                    "dos": "2",
                    "tres": "3",
                    "cuatro": "4",
                    "cinco": "5",
                    "seis": "6",
                    "seís": "6",
                    "séis": "6",
                    "siete": "7",
                    "ocho": "8",
                    "nueve": "9",
                    "diez": "10",
                    "once": "11",
                    "doce": "12",
                    "trece": "13",
                    "dieci": "1",
                    "veinti": "2",
                    "veinte": "20"
                    }
    
    for a, b in replacements.items():
        str_numero = str_numero.replace(a, b)
    return str_numero


def separa_regiones(str_region):
    
    reemplazar_region = {"DECIMA REGION": "REGION",
                         "UNDECIMA REGION": "REGION",
                         "DUODECIMA REGION": "REGION",
                         "DECIMOCUARTA REGION": "REGION",
                         "DECIMOQUINTA REGION": "REGION",
                         "PRIMERA REGION": "REGION",
                         "SEGUNDA REGION": "REGION",
                         "TERCERA REGION": "REGION",
                         "CUARTA REGION": "REGION",
                         "QUINTA REGION": "REGION",
                         "SEXTA REGION": "REGION",
                         "SEPTIMA REGION": "REGION",
                         "OCTAVA REGION": "REGION",
                         "NOVENA REGION": "REGION",
                         "BIOBIO": "REGION DEL BIO BIO",
                         "AYSEN": "REGION DE AISEN",
                         "MAGALLANES Y DE LA ANTARTICA CHILENA": "REGION DE MAGALLANES Y ANTARTICA CHILENA"
                        }
    for old, new in reemplazar_region.items():
        str_region = str_region.replace(old, new)
    return str_region


def transforma_asiento(str_asiento):
    if str_asiento.find("JUZGADO DE GARANTIA") != -1 or str_asiento.find("TRIBUNAL DE JUICIO ORAL EN LO PENAL") != -1:
        str_asiento = "SANTIAGO"
    return str_asiento

def cambio_nombre_juzgados(str_tribunal):
        reemplazar_texto = {"1º JUZGADO DE LETRAS": "JUZGADO DE LETRAS",
                            "6º TRIBUNAL DE JUICIO ORAL EN LO PENAL DE SAN MIGUEL": "SEXTO TRIBUNAL DE JUICIO ORAL EN LO PENAL SANTIAGO",
                            "10º JUZGADO DE GARANTIA": "DECIMO JUZGADO DE GARANTIA",
                            "11º JUZGADO DE GARANTIA": "UNDECIMO JUZGADO DE GARANTIA",
                            "12º JUZGADO DE GARANTIA": "DUODECIMO JUZGADO DE GARANTIA",
                            "13º JUZGADO DE GARANTIA": "DECIMOTERCER JUZGADO DE GARANTIA",
                            "14º JUZGADO DE GARANTIA": "DECIMOCUARTO JUZGADO DE GARANTIA",
                            "15º JUZGADO DE GARANTIA": "DECIMOQUINTO JUZGADO DE GARANTIA",
                            "TRIBUNAL ORAL EN LO PENAL DE": "TRIBUNAL DE JUICIO ORAL EN LO PENAL",
                            "1º": "PRIMER",
                            "2º": "SEGUNDO",
                            "3º": "TERCER",
                            "4º": "CUARTO",
                            "5º": "QUINTO",
                            "6º": "SEXTO",
                            "7º": "SEPTIMO",
                            "8º": "OCTAVO",
                            "9º": "NOVENO",
                            "TRIBUNAL DE JUICIO ORAL EN LO PENAL DE DE ": "TRIBUNAL DE JUICIO ORAL EN LO PENAL ",
                            "TRIBUNAL DE JUICIO ORAL EN LO PENAL DE": "TRIBUNAL DE JUICIO ORAL EN LO PENAL",
                            "JUZGADO DE GARANTIA DE DE ": "JUZGADO DE GARANTIA ",
                            "JUZGADO DE GARANTIA DE": "JUZGADO DE GARANTIA",
                            "JUZGADO DE LETRAS Y GARANTIA DE": "JUZGADO DE LETRAS Y GARANTIA",
                            "JUZGADO DE LETRAS DE": "JUZGADO DE LETRAS Y GARANTIA",
                            "LA CALERA": "CALERA",
                            "PUERTO NATALES": "NATALES",
                            "PUERTO AYSEN": "AISEN",
                            "PUERTO CISNES": "CISNES",
                            "SAN VICENTE DE TAGUA-TAGUA": "SAN VICENTE",
                            "ACHAO": "QUINCHAO",
                            "COYHAIQUE": "COIHAIQUE"
                            }
        
        for old, new in reemplazar_texto.items():
            str_tribunal = str_tribunal.replace(old, new)
        return str_tribunal
    

def cambio_termino_causa(str_termino):
    if pd.notnull(str_termino):
        str_termino = str_termino.replace(".","")
    
    return str_termino

def load_concatenate_by_filename(needle: str, src_path = "data/raw/pjud"):
    archivos = os.listdir(src_path)
    tqdm.pandas()

    dataframes = []

    for archivo in archivos:
        if archivo.find(needle) != -1:
            df = pd.read_csv(f"{src_path}/{archivo}", sep=";", encoding='cp850', dtype='unicode', low_memory=True)
            dataframes.append(df)

    return pd.concat(dataframes)

def carga_limpieza_ingresos_materia():
    df_ingresos_materia = load_concatenate_by_filename('Ingresos por Materia Penal')

    df_ingresos_materia['TOTAL INGRESOS POR MATERIAS'] = df_ingresos_materia['TOTAL INGRESOS POR MATERIAS'].fillna(
        df_ingresos_materia['TOTAL INGRESOS POR MATERIAS(*)'])
    df_ingresos_materia.drop(['N°', 'TOTAL INGRESOS POR MATERIAS(*)'], axis='columns', inplace=True)

    df_ingresos_materia.drop([
                                 '(*)Se agregó columna total de ingresos, dado que en algunas causas, la materia se repite (error de tramitación)'],
                             axis='columns', inplace=True)

    # TRANSFORMAMOS DE FLOAT A INTEGER

    df_ingresos_materia['COD. CORTE'] = df_ingresos_materia['COD. CORTE'].fillna(0).astype(np.int16)
    df_ingresos_materia['COD. TRIBUNAL'] = df_ingresos_materia['COD. TRIBUNAL'].fillna(0).astype(np.int16)
    df_ingresos_materia['COD. MATERIA'] = df_ingresos_materia['COD. MATERIA'].fillna(0).astype(np.int16)
    df_ingresos_materia['AÑO INGRESO'] = df_ingresos_materia['AÑO INGRESO'].fillna(0).astype(np.int16)
    df_ingresos_materia['TOTAL INGRESOS POR MATERIAS'] = df_ingresos_materia['TOTAL INGRESOS POR MATERIAS'].fillna(0).astype(np.int8)

    # Transformamos fechas
    click.echo('Transformando fechas')

    df_ingresos_materia['FECHA INGRESO'] = df_ingresos_materia['FECHA INGRESO'].progress_apply(convierte_fecha)

    # Elimino espacios en las columnas tipo objetos
    click.echo('Eliminando espacios en objetos')

    df_ingresos_materia = df_ingresos_materia.progress_apply(elimina_espacios, axis=0)

    # Elimino tildes

    click.echo('Eliminando tildes')

    cols = df_ingresos_materia.select_dtypes(include=["object"]).columns
    df_ingresos_materia[cols] = df_ingresos_materia[cols].progress_apply(elimina_tilde)

    # Categorizacion

    df_ingresos_materia['CORTE'] = df_ingresos_materia['CORTE'].astype('category')

    tipo_causa = df_ingresos_materia[df_ingresos_materia['TIPO CAUSA'] != 'Ordinaria']

    df_ingresos_materia.drop(tipo_causa.index, axis=0, inplace=True)

    data.save_feather(df_ingresos_materia, 'IngresosMateria')

