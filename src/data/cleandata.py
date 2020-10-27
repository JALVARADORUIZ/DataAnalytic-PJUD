import pandas as pd
from datetime import datetime
from datetime import timedelta


def convierte_fecha(fecha): 
    try:
        day,month,year = map(int,fecha.split(sep = "-"))
    except:
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
