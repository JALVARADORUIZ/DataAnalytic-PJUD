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

def obtiene_año(rit): 
    try:
        rol,year = map(int,rit.split(sep = "-"))
    except:
        #print(f"no pude ejecutar {fecha}")
        return pd.NaT
    
    return (year) 

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

    data.save_feather(df_ingresos_materia, 'clean_IngresosMateria')

    click.echo("Generado archivo Feather 'clean_IngresosMateria.feather'. Proceso Terminado")

def carga_limpieza_terminos_materia():
    df_termino_materia = load_concatenate_by_filename('Términos por Materia Penal')

    df_metge = df_termino_materia[df_termino_materia['SISTEMA']=='METGE']
    df_termino_materia.drop(df_metge.index, axis=0, inplace=True)

    # Estandarización de nombres de variables

    df_termino_materia.rename(columns = {'CÓD. CORTE':'COD. CORTE',
                                        'CÓD. TRIBUNAL':'COD. TRIBUNAL',
                                        'CÓD. MATERIA':'COD. MATERIA',
                                        'MOTIVO DE TÉRMINO':'MOTIVO TERMINO',
                                        'DURACIÓN CAUSA':'DURACION CAUSA',
                                        'FECHA TÉRMINO':'FECHA TERMINO',
                                        'MES TÉRMINO':'MES TERMINO',
                                        'AÑO TÉRMINO':'AÑO TERMINO',
                                        'TOTAL TÉRMINOS':'TOTAL TERMINOS'
                                        },inplace = True)

    df_termino_materia.drop(['N°','SISTEMA'], axis = 'columns', inplace = True)

    # TRANSFORMAMOS DE FLOAT A INTEGER

    df_termino_materia['COD. CORTE'] = df_termino_materia['COD. CORTE'].fillna(0).astype(np.int16)
    df_termino_materia['COD. TRIBUNAL'] = df_termino_materia['COD. TRIBUNAL'].fillna(0).astype(np.int16)
    df_termino_materia['COD. MATERIA'] = df_termino_materia['COD. MATERIA'].fillna(0).astype(np.int16)
    df_termino_materia['DURACION CAUSA'] = df_termino_materia['DURACION CAUSA'].fillna(0).astype(np.int16)
    df_termino_materia['AÑO TERMINO'] = df_termino_materia['AÑO TERMINO'].fillna(0).astype(np.int16)
    df_termino_materia['TOTAL TERMINOS'] = df_termino_materia['TOTAL TERMINOS'].fillna(0).astype(np.int8)

    # Transformamos formato fecha

    click.echo('Convirtiendo fechas')
    df_termino_materia['FECHA INGRESO'] = df_termino_materia['FECHA INGRESO'].progress_apply(convierte_fecha)
    df_termino_materia['FECHA TERMINO'] = df_termino_materia['FECHA TERMINO'].progress_apply(convierte_fecha)

    # Elimino espacios en las columnas tipo objetos

    click.echo('Eliminando espacios')
    df_termino_materia = df_termino_materia.progress_apply(elimina_espacios, axis=0)

    # Elimino tildes de object

    click.echo('Eliminando tilde')
    cols = df_termino_materia.select_dtypes(include = ["object"]).columns
    df_termino_materia[cols] = df_termino_materia[cols].progress_apply(elimina_tilde)

    # Limpieza de RIT 
    click.echo('Limpieza de RIT')
    df_termino_materia['RIT'] = df_termino_materia['RIT'].progress_apply(limpia_rit)

    # Categorizar variables

    df_termino_materia['CORTE'] = df_termino_materia['CORTE'].astype('category')
    df_termino_materia['MOTIVO TERMINO'] = df_termino_materia['MOTIVO TERMINO'].astype('category')
    
    #Dejo solo causas Ordinarias

    tipo_causa = df_termino_materia[df_termino_materia['TIPO CAUSA']!='Ordinaria']
    df_termino_materia.drop(tipo_causa.index, axis=0, inplace=True)

    # Reset el index para realizar feather

    data.save_feather(df_termino_materia, 'clean_TerminosMateria')

    click.echo("Generado archivo Feather 'clean_TerminosMateria.feather'. Proceso Terminado")

def carga_limpieza_ingresos_rol():
    df_ingresos_rol = load_concatenate_by_filename('Ingresos por Rol Penal')

    # Transformamos variables float64 a int16

    df_ingresos_rol['COD. CORTE'] = df_ingresos_rol['COD. CORTE'].fillna(0).astype(np.int16)
    df_ingresos_rol['COD. TRIBUNAL'] = df_ingresos_rol['COD. TRIBUNAL'].fillna(0).astype(np.int16)
    df_ingresos_rol['AÑO INGRESO'] = df_ingresos_rol['AÑO INGRESO'].fillna(0).astype(np.int16)
    df_ingresos_rol.drop(['N°'], axis = 'columns', inplace = True)

    click.echo('Transformando Fechas')
    df_ingresos_rol['FECHA INGRESO'] = df_ingresos_rol['FECHA INGRESO'].progress_apply(convierte_fecha)

    click.echo('Eliminando espacios en columnas objetos')
    df_ingresos_rol = df_ingresos_rol.progress_apply(elimina_espacios, axis=0)

    click.echo('Eliminando tildes')
    cols = df_ingresos_rol.select_dtypes(include = ["object"]).columns
    df_ingresos_rol[cols] = df_ingresos_rol[cols].progress_apply(elimina_tilde)

    # Transformamos en variables categoricas

    df_ingresos_rol['CORTE'] = df_ingresos_rol['CORTE'].astype('category')

    # Elimina de causas que no sean del tipo ordinaria
    tipo_causa = df_ingresos_rol[df_ingresos_rol['TIPO CAUSA']!='Ordinaria']
    df_ingresos_rol.drop(tipo_causa.index, axis=0, inplace=True)

    data.save_feather(df_ingresos_rol,'clean_IngresosRol')

    click.echo("Generado archivo Feather 'clena_IngresosRol.feather'. Proceso Terminado")

def carga_limpieza_terminos_rol():
    df_termino_rol = load_concatenate_by_filename('Términos por Rol Penal')

    # Elimino causas que no sean SIAGJ
    df_no_siagj = df_termino_rol[df_termino_rol['SISTEMA']!='SIAGJ']
    df_termino_rol.drop(df_no_siagj.index, axis=0, inplace=True)

    # Elimino filas vacias o con datos NaN
    df_termino_rol = df_termino_rol.dropna()
    df_termino_rol.drop(['N°','SISTEMA'], axis = 'columns', inplace = True)

    # Cambio de nombre a algunas columnas para dejarlas iguales a otros dataframes

    df_termino_rol.rename(columns = {'CÓD. CORTE':'COD. CORTE',
                                'CÓD. TRIBUNAL':'COD. TRIBUNAL',
                                'DURACIÓN CAUSA ':'DURACION CAUSA',
                                'MOTIVO DE TÉRMINO':'MOTIVO TERMINO',
                                'FECHA TÉRMINO':'FECHA TERMINO',
                                'MES TÉRMINO':'MES TERMINO',
                                'AÑO TÉRMINO':'AÑO TERMINO',
                                'TOTAL TÉRMINOS':'TOTAL TERMINOS'
                                }, inplace = True) 
    # Transformamos variables float64 a int16

    df_termino_rol['COD. CORTE'] = df_termino_rol['COD. CORTE'].fillna(0).astype(np.int16)
    df_termino_rol['COD. TRIBUNAL'] = df_termino_rol['COD. TRIBUNAL'].fillna(0).astype(np.int16)
    df_termino_rol['DURACION CAUSA'] = df_termino_rol['DURACION CAUSA'].fillna(0).astype(np.int16)
    df_termino_rol['AÑO TERMINO'] = df_termino_rol['AÑO TERMINO'].fillna(0).astype(np.int16)
    df_termino_rol['TOTAL TERMINOS'] = df_termino_rol['TOTAL TERMINOS'].fillna(0).astype(np.int8)

    click.echo('Elimino tildes de las columnas object')
    cols = df_termino_rol.select_dtypes(include = ["object"]).columns
    df_termino_rol[cols] = df_termino_rol[cols].progress_apply(elimina_tilde)

    click.echo('Transformando fechas')
    df_termino_rol['FECHA INGRESO'] = df_termino_rol['FECHA INGRESO'].progress_apply(convierte_fecha)
    df_termino_rol['FECHA TERMINO'] = df_termino_rol['FECHA TERMINO'].progress_apply(convierte_fecha) 

    click.echo('Elimino espacios en las columnas tipo objeto')
    df_termino_rol = df_termino_rol.progress_apply(elimina_espacios, axis=0)

    click.echo('Limpieza de RIT')
    df_termino_rol['RIT'] = df_termino_rol['RIT'].progress_apply(limpia_rit)

    # Transformamos en variables categoricas

    df_termino_rol['CORTE'] = df_termino_rol['CORTE'].astype('category')
    df_termino_rol['MOTIVO TERMINO'] = df_termino_rol['MOTIVO TERMINO'].astype('category')

    # Dejo solo causas Ordinarias
    tipo_causa = df_termino_rol[df_termino_rol['TIPO CAUSA']!='Ordinaria']
    df_termino_rol.drop(tipo_causa.index, axis=0, inplace=True)

    data.save_feather(df_termino_rol,'clean_TerminosRol')

    click.echo("Generado archivo Feather clean_TerminosRol.feather'. Proceso Terminado")

def carga_limpieza_inventario():
    df_inventario = load_concatenate_by_filename('Inventario Causas en Tramitación Penal')

    # Elimino registros de METGE
    df_metge = df_inventario[df_inventario['SISTEMA']=='METGE']
    df_inventario.drop(df_metge.index, axis=0, inplace=True)

    # ESTANDARIZACION DE NOMBRES DE VARIABLES 

    df_inventario.rename(columns = {'CÓDIGO CORTE':'COD. CORTE',
                                    'CÓDIGO TRIBUNAL':'COD. TRIBUNAL',
                                    'CÓDIGO MATERIA':'COD. MATERIA',
                                    ' MATERIA':'MATERIA'
                                    }, inplace = True)

    df_inventario.drop(['SISTEMA'], axis = 'columns', inplace = True)

    # TRANSFORMAMOS DE FLOAT A INTEGER

    df_inventario['COD. CORTE'] = df_inventario['COD. CORTE'].fillna(0).astype(np.int16)
    df_inventario['COD. TRIBUNAL'] = df_inventario['COD. TRIBUNAL'].fillna(0).astype(np.int16)
    df_inventario['COD. MATERIA'] = df_inventario['COD. MATERIA'].fillna(0).astype(np.int16)
    df_inventario['TOTAL INVENTARIO'] = df_inventario['TOTAL INVENTARIO'].fillna(0).astype(np.int8)

    
    click.echo('Transformamos fechas')
    df_inventario['FECHA INGRESO'] = df_inventario['FECHA INGRESO'].progress_apply(convierte_fecha)
    df_inventario['FECHA ULT. DILIGENCIA'] = df_inventario['FECHA ULT. DILIGENCIA'].progress_apply(convierte_fecha)

    click.echo('Elimino espacios en las columnas tipo objetos')
    df_inventario = df_inventario.progress_apply(elimina_espacios, axis=0)

    click.echo('Elimino tildes de las columnas object')
    cols = df_inventario.select_dtypes(include = ["object"]).columns
    df_inventario[cols] = df_inventario[cols].progress_apply(elimina_tilde)

    # CATEGORIZACION DE VARIABLES

    df_inventario['CORTE'] = df_inventario['CORTE'].astype('category')
    df_inventario['COMPETENCIA'] = df_inventario['COMPETENCIA'].astype('category')
    df_inventario['TIPO ULT. DILIGENCIA'] = df_inventario['TIPO ULT. DILIGENCIA'].astype('category')

    # Dejo solo causas Ordinarias
    tipo_causa = df_inventario[df_inventario['TIPO CAUSA']!='Ordinaria']
    df_inventario.drop(tipo_causa.index, axis=0, inplace=True)

    data.save_feather(df_inventario,'clean_Inventario')

    click.echo("Generado archivo Feather 'clean_Inventario.feather'. Proceso Terminado")

def carga_limpieza_audiencias():
    df_audiencias = load_concatenate_by_filename('Audiencias Realizadas Penal')

    df_audiencias.rename(columns = {'CÓD. CORTE':'COD. CORTE',
                                    'CÓD. TRIBUNAL':'COD. TRIBUNAL',
                                    'DURACIÓN  AUDIENCIA':'DURACION AUDIENCIA',
                                    'AGENDAMIENTO (DÍAS CORRIDOS)':'DIAS AGENDAMIENTO',
                                    'DURACIÓN AUDIENCIA (MINUTOS)':'DURACION AUDIENCIA (MIN)',
                                    'FECHA PROGRAMACIÓN AUDIENCIA':'FECHA PROGRAMACION AUDIENCIA'
                                },
                        inplace = True)

    # TRANSFORMAMOS DE FLOAT A INTEGER

    df_audiencias['COD. CORTE'] = df_audiencias['COD. CORTE'].fillna(0).astype(np.int16)
    df_audiencias['COD. TRIBUNAL'] = df_audiencias['COD. TRIBUNAL'].fillna(0).astype(np.int16)
    df_audiencias['TOTAL AUDIENCIAS'] = df_audiencias['TOTAL AUDIENCIAS'].fillna(0).astype(np.int8)

    # Podemos observar que existen columnas que se repiten, y que tienen datos NAN en algunas pero esos datos 
    # en otras columnas, pasa en TIPO AUDIENCIA=TIPO DE AUDIENCIA, AGENDAMIENTO (DÍAS CORRIDOS)=PLAZO AGENDAMIENTO
    # (DÍAS CORRIDOS), DURACIÓN AUDIENCIA (MINUTOS)= DURACIÓN AUDIENCIA

    df_audiencias['TIPO DE AUDIENCIA'] = df_audiencias['TIPO DE AUDIENCIA'].fillna(df_audiencias['TIPO AUDIENCIA'])
    df_audiencias['DIAS AGENDAMIENTO'] = df_audiencias['DIAS AGENDAMIENTO'].fillna(df_audiencias['PLAZO AGENDAMIENTO (DIAS CORRIDOS)']).astype(np.int16)
    df_audiencias['DURACION AUDIENCIA (MIN)'] = df_audiencias['DURACION AUDIENCIA (MIN)'].fillna(df_audiencias['DURACION AUDIENCIA'])

    # Elimino las columnas reemplazadas

    df_audiencias.drop(['TIPO AUDIENCIA','PLAZO AGENDAMIENTO (DIAS CORRIDOS)','DURACION AUDIENCIA'], axis = 'columns', 
                        inplace = True)

    click.echo('Transformamos fechas')

    df_audiencias['FECHA PROGRAMACION AUDIENCIA'] = df_audiencias['FECHA PROGRAMACION AUDIENCIA'].progress_apply(convierte_fecha)
    df_audiencias['FECHA AUDIENCIA'] = df_audiencias['FECHA AUDIENCIA'].progress_apply(convierte_fecha)

    click.echo('Elimino espacios en las columnas tipo objetos')

    df_audiencias = df_audiencias.progress_apply(elimina_espacios, axis=0)
    
    click.echo('Elimino tildes') 
    cols = df_audiencias.select_dtypes(include = ["object"]).columns
    df_audiencias[cols] = df_audiencias[cols].progress_apply(elimina_tilde)

    # Categorizar

    df_audiencias['CORTE'] = df_audiencias['CORTE'].astype('category')

    # Dejo solo causas Ordinarias
    tipo_causa = df_audiencias[df_audiencias['TIPO CAUSA']!='Ordinaria']
    df_audiencias.drop(tipo_causa.index, axis=0, inplace=True)

    data.save_feather(df_audiencias,'clean_Audiencias')

    click.echo("Generado archivo Feather 'clean_Audiencias.feather'. Proceso Terminado")

def carga_limpieza_duraciones():
    df_duraciones = load_concatenate_by_filename('Duraciones por Rol Penal')

    # Elimino causas que no sean SIAGJ
    df_no_siagj = df_duraciones[df_duraciones['SISTEMA']!='SIAGJ']
    df_duraciones.drop(df_no_siagj.index, axis=0, inplace=True)

    df_duraciones.rename(columns = {'CÓD. CORTE':'COD. CORTE',
                                    'CÓD. TRIBUNAL':'COD. TRIBUNAL',
                                    'DURACIÓN CAUSA ':'DURACIÓN CAUSA',
                                    'FECHA TÉRMINO':'FECHA TERMINO',
                                    'MOTIVO DE TÉRMINO':'MOTIVO TERMINO',
                                    'MES TÉRMINO':'MES TERMINO',
                                    'AÑO TÉRMINO':'AÑO TERMINO',
                                    'TOTAL TÉRMINOS':'TOTAL TERMINOS'
                                    }, inplace = True)

    df_duraciones.drop(['N°','SISTEMA'], axis = 'columns', inplace = True)
    df_duraciones = df_duraciones.dropna()

    # TRANSFORMAMOS DE FLOAT A INTEGER

    df_duraciones['COD. CORTE'] = df_duraciones['COD. CORTE'].fillna(0).astype(np.int16)
    df_duraciones['COD. TRIBUNAL'] = df_duraciones['COD. TRIBUNAL'].fillna(0).astype(np.int16)
    df_duraciones['AÑO TERMINO'] = df_duraciones['AÑO TERMINO'].fillna(0).astype(np.int16)
    df_duraciones['TOTAL TERMINOS'] = df_duraciones['TOTAL TERMINOS'].fillna(0).astype(np.int8)

    click.echo('Transformamos fechas')

    df_duraciones['FECHA INGRESO'] = df_duraciones['FECHA INGRESO'].progress_apply(convierte_fecha)
    df_duraciones['FECHA TERMINO'] = df_duraciones['FECHA TERMINO'].progress_apply(convierte_fecha)

    click.echo('Elimino espacios en las columnas tipo objetos')

    df_duraciones = df_duraciones.progress_apply(elimina_espacios, axis=0)

    click.echo('Elimino tildes')
    cols = df_duraciones.select_dtypes(include = ["object"]).columns
    df_duraciones[cols] = df_duraciones[cols].progress_apply(elimina_tilde) 

    click.echo('Transformar el formato del RIT--AÑO a RIT-AÑO')
    df_duraciones['RIT'] = df_duraciones['RIT'].progress_apply(limpia_rit)

    # Categorizacion

    df_duraciones['CORTE'] = df_duraciones['CORTE'].astype('category')
    df_duraciones['MOTIVO TERMINO'] = df_duraciones['MOTIVO TERMINO'].astype('category')

    # Dejo solo causas Ordinarias
    tipo_causa = df_duraciones[df_duraciones['TIPO CAUSA']!='Ordinaria']
    df_duraciones.drop(tipo_causa.index, axis=0, inplace=True)

    data.save_feather(df_duraciones, 'clean_Duraciones')
    click.echo("Generado archivo Feather 'clean_Duraciones.feather'. Proceso Terminado")

def carga_limpieza_delitos():
    tqdm.pandas()
    path_raw = "data/raw/delitos"
    codigos_delitos = pd.read_excel(f"{path_raw}/codigos_penal_2020.xlsx", sheet_name = "codigos vigentes", engine='openpyxl')
    
    # elimino filas con NaN
    codigos_delitos = codigos_delitos.drop_duplicates() 
    # elimino 2 primeras filas que son titulos
    codigos_delitos = codigos_delitos.drop([0,1,2], axis = 0) 

    # elimino columnas con datos NaN

    variables = range(2,248)
    columnas = []

    for variable in variables:
        columnas.append("Unnamed: " + str(variable))

    codigos_delitos = codigos_delitos.drop(columns = columnas, axis = 1)
    
    # cambio nombres columnas
    codigos_delitos = codigos_delitos.rename(columns = {'VERSION AL 01/01/2018':'COD. MATERIA', 'Unnamed: 1':'MATERIA'})    

    delitos_vigentes = []

    for item in codigos_delitos.index:
        if str(codigos_delitos['COD. MATERIA'][item]).isupper():
            tipologia_delito=str(codigos_delitos['COD. MATERIA'][item])
        else:
            delitos_vigentes.append([codigos_delitos['COD. MATERIA'][item],
                                    str(codigos_delitos['MATERIA'][item]).upper().rstrip(),
                                    tipologia_delito,'VIGENTE'])
    
    df_delitos_vigentes = pd.DataFrame(delitos_vigentes,columns = ['COD. MATERIA','MATERIA','TIPOLOGIA MATERIA','VIGENCIA MATERIA'])

    click.echo('Elimino tildes de las columnas object')

    cols = df_delitos_vigentes.select_dtypes(include = ["object"]).columns
    df_delitos_vigentes[cols] = df_delitos_vigentes[cols].progress_apply(elimina_tilde)
    df_delitos_vigentes[cols] = df_delitos_vigentes[cols].progress_apply(limpieza_caracteres)

    df_delitos_vigentes['COD. MATERIA'] = df_delitos_vigentes['COD. MATERIA'].fillna(0).astype('int16')

    # CARGA Y LIMPIEZA DE DATOS RELACIONADOS A DELITOS NO VIGENTES
    codigos_delitos_novigentes = pd.read_excel(f"{path_raw}/codigos_penal_2020.xlsx", sheet_name = "Codigos no vigentes", engine='openpyxl')

    # cambio nombres columnas

    codigos_delitos_novigentes = codigos_delitos_novigentes.rename(columns = {'MATERIAS PENALES NO VIGENTES':'TIPOLOGIA MATERIA',
                                                                            'Unnamed: 1':'COD. MATERIA','Unnamed: 2':'MATERIA'})
    # elimino primera fila que son titulos
    codigos_delitos_novigentes = codigos_delitos_novigentes.drop([0], axis = 0) 
    # reemplazo Nan por ST
    codigos_delitos_novigentes = codigos_delitos_novigentes.fillna('ST') 

    delitos_no_vigentes = []
    for item in codigos_delitos_novigentes.index:
        
        tipologia_delito = codigos_delitos_novigentes['TIPOLOGIA MATERIA'][item]
        
        if tipologia_delito != 'ST':
            tipologia = codigos_delitos_novigentes['TIPOLOGIA MATERIA'][item]
        else:
            tipologia_delito = tipologia
        
        delitos_no_vigentes.append([codigos_delitos_novigentes['COD. MATERIA'][item],
                                    codigos_delitos_novigentes['MATERIA'][item].rstrip(),
                                    tipologia_delito,'NO VIGENTE'])

    df_delitos_no_vigentes = pd.DataFrame(delitos_no_vigentes, columns = ['COD. MATERIA','MATERIA','TIPOLOGIA MATERIA','VIGENCIA MATERIA'])

    click.echo('Elimino tildes de las columnas object')

    cols = df_delitos_no_vigentes.select_dtypes(include = ["object"]).columns
    df_delitos_no_vigentes[cols] = df_delitos_no_vigentes[cols].progress_apply(elimina_tilde)

    df_delitos_no_vigentes['COD. MATERIA'] = df_delitos_no_vigentes['COD. MATERIA'].astype('int16')

    # UNION DE AMBOS DATASET CON DELITOS VIGENTES Y NO VIGENTES
    df_delitos = pd.concat([df_delitos_vigentes,df_delitos_no_vigentes])

    data.save_feather(df_delitos,'clean_Delitos',path='data/processed/delitos')
    click.echo("Generado archivo Feather 'clean_Delitos.feather'. Proceso Terminado")