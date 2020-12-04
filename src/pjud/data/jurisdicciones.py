import re
import os
import pandas as pd
import numpy as np
import click

from tqdm import tqdm
from unicodedata import normalize

from pjud import data

def load_article_cot(article: str, src_path = './data/raw/cot'):
    tqdm.pandas()
    with open(f'{src_path}/{article}.txt', 'r') as file:
        contenido = ''
        for line in file.readlines():
            contenido += line
    return contenido

def garantia():
    regex_jg=r"(?:(?P<Region>^[\w \']+)\:\n\n)|(?P<JG>^[\w. \-]+)\,\scon\s(?P<Jueces>[\w.\-]+)[a-z\-\s\,]+(?P<Competencia>\.|\s[\w. \-\,]+)"
    matches = re.findall(regex_jg, load_article_cot('Juzgados_Garantia'), re.MULTILINE)

    data_jg = []

    for item in range(0, len(matches)):
        if matches[item][0] != '':
            region = matches[item][0].upper()
        else:
            if matches[item][1] != '':
                ciudad = matches[item][1].upper()
                if ciudad.find("JUZGADO") != -1:
                    juzgado = ciudad

                else:
                    juzgado = f"JUZGADO DE GARANTIA {ciudad}"

            if matches[item][2] != '':
                cantidad_jueces = data.cleandata.transforma_numero(matches[item][2])

            if matches[item][3] == '.':
                competencia = ciudad

            else:
                if matches[item][3] != '':
                    competencia = matches[item][3].upper()

            competencia = competencia.replace(" Y ", ",")
            competencia = competencia.replace(" E ", ",")
            competencia = competencia.replace(".", "")

            comunas = competencia.split(",")

            for comuna in comunas:
                data_jg.append([region, juzgado, ciudad, cantidad_jueces, comuna.strip(), 'GARANTIA'])

    df_juzgados_garantia = pd.DataFrame(data_jg,
                                        columns=['REGION', 'TRIBUNAL', 'ASIENTO', 'JUECES', 'COMUNA', 'TIPO JUZGADO'])

    # Elimino tildes de las columnas object

    cols = df_juzgados_garantia.select_dtypes(include = ["object"]).columns
    df_juzgados_garantia[cols] = df_juzgados_garantia[cols].progress_apply(data.cleandata.elimina_tilde)

    data.save_feather(df_juzgados_garantia, 'JuzgadosGarantia')
    click.echo('Generado archivo Feather. Proceso Terminado')

def top():
    # Se construye una expresion regular para captar una lista con la información que se desea procesar, y así generar un dataframe.
    regex_top=r"(?:(?P<Region>^[\w \']+)\:\n)|(?P<JG>^[\w. \-]+)\,\scon\s(?P<Jueces>[\w.\-]+)[a-z\-\s\,]+(?P<Competencia>\.|\s[\w. \-\,\']+)"
    matches = re.findall(regex_top, load_article_cot('Tribunales_orales'), re.MULTILINE)

    data_top=[]

    for item in range(0,len(matches)):
        if matches[item][0] != '':
            region = matches[item][0].upper()
        else:    
            if matches[item][1] != '':
                ciudad = matches[item][1].upper() 
                if ciudad.find("TRIBUNAL") != -1:
                    juzgado = ciudad
                    
                else:
                    juzgado = f"TRIBUNAL DE JUICIO ORAL EN LO PENAL {ciudad}"

            if matches[item][2] != '':
                cantidad_jueces = data.cleandata.transforma_numero(matches[item][2])
        
            if matches[item][3] == '.':
                competencia = ciudad
            
            else:  
                if matches[item][3] != '':
                    competencia = matches[item][3].upper()
            
            competencia = competencia.replace(" Y ",",")
            competencia = competencia.replace(" E ",",")
            competencia = competencia.replace(".","")
            
            comunas = competencia.split(",")
            
            for comuna in comunas:
                data_top.append([region,juzgado,ciudad,cantidad_jueces,comuna.strip(),'ORAL'])
    
    df_tribunal_oral = pd.DataFrame(data_top, columns = ['REGION','TRIBUNAL','ASIENTO','JUECES','COMUNA','TIPO JUZGADO'])

    df_tribunal_oral['JUECES'] = df_tribunal_oral['JUECES'].fillna(0).astype(np.int8)

    click.echo('Eliminando tildes')
    cols = df_tribunal_oral.select_dtypes(include = ["object"]).columns
    df_tribunal_oral[cols] = df_tribunal_oral[cols].progress_apply(data.cleandata.elimina_tilde)

    data.save_feather(df_tribunal_oral,'TribunalOral')
    click.echo('Generado archivo Feather. Proceso Terminado')

def juzgados_letras():
    regex_jl = r"(?:Art.\s[0-9\s(bis|ter|quáter)]+\.\s[\w\s]+\,(?P<Region>[\w\s\']+)\,[\w\s]+:\s+)|(?:(?:^[A]\.\-\s[\w\s]+:\s+)(?:(?:[\w\s\,]+[.|\;])+))|(?:(?:[\-\s][\w\s]+\:\s*)|(?:\s*(?:(?P<cant_juzg>[\w]+)(?:\s[J|j][a-z\,\s]+)(?P<JG>[\w\s]+)[\,|y]\s[\w]+\s(?P<Jueces>[\w]+)\s[a-z\s\,]+(?P<Competencia>[\w|\s|\,]+)[\;|\.]$)\s*))"
    matches = re.findall(regex_jl, load_article_cot('juzgadoletras'), re.MULTILINE)

    data_jl = []

    for item in range(0,len(matches)):

        if matches[item][0] != '':
            region = f"REGION{matches[item][0].upper()}"
        else:    
            if matches[item][2] != '':
                ciudad = matches[item][2].upper() 
                juzgado = f"JUZGADO DE LETRAS Y GARANTIA {ciudad}"

            if matches[item][3] != '':
                if matches[item][3] == 'competencia':
                    cantidad_jueces = 1
                else:
                    cantidad_jueces = data.cleandata.transforma_numero(matches[item][3])
        
            if matches[item][4] != '':
                if matches[item][4] == 'a':
                    competencia = ciudad
            
                else:  
                    if matches[item][4] != '':
                        competencia = matches[item][4].upper()

                competencia = competencia.replace(" Y ",",")
                competencia = competencia.replace(" E ",",")
                competencia = competencia.replace(".","")
            
                comunas = competencia.split(",")
            
                for comuna in comunas:
                    data_jl.append([region,juzgado,ciudad,cantidad_jueces,comuna.strip(),'LETRAS Y GARANTIA'])


    df_juzgados_letras = pd.DataFrame(data_jl,columns = ['REGION','TRIBUNAL','ASIENTO','JUECES','COMUNA','TIPO JUZGADO'])

    click.echo('Elimando tildes')
    cols = df_juzgados_letras.select_dtypes(include = ["object"]).columns
    df_juzgados_letras[cols] = df_juzgados_letras[cols].progress_apply(data.cleandata.elimina_tilde)

    data.save_feather(df_juzgados_letras, 'JuzgadosLetras')
    click.echo('Generado archivo Feather. Proceso Terminado')