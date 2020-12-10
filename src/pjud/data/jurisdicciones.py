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

    data.save_feather(df_juzgados_garantia, 'generates_JuzgadosGarantia')
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

    data.save_feather(df_tribunal_oral,'generates_TribunalOral')
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

    data.save_feather(df_juzgados_letras, 'generates_JuzgadosLetras')
    click.echo('Generado archivo Feather. Proceso Terminado')

def extraccion_comunas(filtro, df):
    add_comunas = []

    for indice in filtro.index:
        region = filtro["REGION"][indice]
        tribunal = filtro["TRIBUNAL"][indice]
        asiento = filtro["ASIENTO"][indice]
        jueces = filtro["JUECES"][indice]
        tipo_juzgado = filtro["TIPO JUZGADO"][indice]
        
        provincia = filtro["COMUNA"][indice].split("PROVINCIA DE ")
        comunas_de_provincias = df.loc[df["Nombre Provincia"] == provincia[1],"Nombre Comuna"]
        
        for var_comuna in range(len(comunas_de_provincias)):
            comuna = comunas_de_provincias.values[var_comuna]
            add_comunas.append([region,tribunal,asiento,jueces,comuna,tipo_juzgado])
    
    return(add_comunas)

def juzgados_penales(path_pjud = "data/interim/pjud", path_subdere = "data/interim/subdere"):

    tqdm.pandas()
    df_juzgados_garantia = pd.read_feather(f"{path_pjud}/generates_JuzgadosGarantia.feather")
    df_tribunal_oral = pd.read_feather(f"{path_pjud}/generates_TribunalOral.feather")
    df_juzgados_letras = pd.read_feather(f"{path_pjud}/generates_JuzgadosLetras.feather")
    df_provincias = pd.read_feather(f"{path_subdere}/generates_Provincias.feather")

    # Existen dos provincias en el listado.
    filtro_provincia_top = df_tribunal_oral[df_tribunal_oral['COMUNA'].str.contains("PROVINCIA", case = False)]
    df_tribunal_oral.drop(filtro_provincia_top.index, axis = 0, inplace = True)

    # Analizo Dataframe , ya que acá estan los Juzgados de Letras con Competencia Común, y en algunos casos estos pueden
    #  No corresponder a Juzgados Penales. Para saber eso, debo verificar que no existan JG en esas comunas, en el caso de 
    # existir debo eliminar registro.

    filtro_provincia_jl = df_juzgados_letras[df_juzgados_letras['COMUNA'].str.contains("PROVINCIA", case = False)]
    df_juzgados_letras.drop(filtro_provincia_jl.index, axis = 0, inplace = True)

    filtro_provincia_jl.loc[229,"COMUNA"] = 'PROVINCIA DE MELIPILLA'
    filtro_provincia_jl.drop(176, inplace = True)

    df_nuevas_comunas_jl = pd.DataFrame(extraccion_comunas(filtro_provincia_jl, df_provincias),
                                        columns = ['REGION','TRIBUNAL','ASIENTO','JUECES','COMUNA','TIPO JUZGADO'])     

    df_juzgados_letras = df_juzgados_letras.append(df_nuevas_comunas_jl, ignore_index = True)

    # Exploro las provincias y extraigo las comunas -> Caso Tribunales orales

    df_nuevas_comunas_top = pd.DataFrame(extraccion_comunas(filtro_provincia_top, df_provincias),
                                        columns = ['REGION','TRIBUNAL','ASIENTO','JUECES','COMUNA','TIPO JUZGADO'])  

    df_tribunal_oral = df_tribunal_oral.append(df_nuevas_comunas_top, ignore_index = True)

    # Acá debo eliminar los juzgados de letras que estan cubiertos por la competencia de un juzgado de Garantía

    juzgados_garantias = df_juzgados_garantia['COMUNA'].unique().tolist()
    drop_jl = []

    for jl in df_juzgados_letras.index:
        if df_juzgados_letras['COMUNA'][jl] in juzgados_garantias:
            drop_jl.append(jl)

    df_juzgados_letras.drop(drop_jl, axis = 0, inplace = True)

    df_juzgados_penales = pd.concat([df_juzgados_garantia,df_juzgados_letras], join = "inner")
    df_juzgados_penales.reset_index(inplace = True)

    # El caso de Alto Hospicio es particular, no aparece en el COT, ya que fue parte de una actualizacion 
    # de la Ley y fue sumado como Juzgado especial. Debemos agregarlo a Juzgados Penales

    alto_hospicio = [('REGION DE TARAPACA','JUZGADO DE LETRAS Y GARANTIA ALTO HOSPICIO','ALTO HOSPICIO','4',
                 'ALTO HOSPICIO','LETRAS Y GARANTIA')]

    df_alto_hospicio = pd.DataFrame(alto_hospicio, columns = ['REGION','TRIBUNAL','ASIENTO','JUECES','COMUNA','TIPO JUZGADO']) 
    df_juzgados_penales = pd.concat([df_juzgados_penales, df_alto_hospicio], join = "inner")
    df_juzgados_penales['JUECES'] = df_juzgados_penales['JUECES'].fillna(0).astype(np.int8)
    df_juzgados_penales.reset_index(drop = True)

    # Transformo las variables para que queden igual en los 3 datasets
    click.echo('Tranformación ... Separando regiones')
    df_juzgados_penales['REGION'] = df_juzgados_penales['REGION'].progress_apply(data.cleandata.separa_regiones)
    df_tribunal_oral['REGION'] = df_tribunal_oral['REGION'].progress_apply(data.cleandata.separa_regiones)

    click.echo('Tranformación ... Creando asiento de tribunales')
    df_juzgados_penales['ASIENTO'] = df_juzgados_penales['ASIENTO'].progress_apply(data.cleandata.transforma_asiento)
    df_tribunal_oral['ASIENTO'] = df_tribunal_oral['ASIENTO'].progress_apply(data.cleandata.transforma_asiento)

    # Transformo valores de Regiones a similar a BBDD Pjud

    regiones_subdere = df_provincias["Nombre Región"].unique()
    regiones_top = df_tribunal_oral["REGION"].unique()

    for region in regiones_subdere:
        for region_top in regiones_top:
            if region_top.find(region) != -1:
                df_provincias['Nombre Región'] = df_provincias['Nombre Región'].replace(region, region_top) 

    click.echo('Tranformación ... Separando regiones') 
    df_provincias['Nombre Región'] = df_provincias['Nombre Región'].progress_apply(data.cleandata.separa_regiones)

    # Creare dataframe con tribunales ORales- Garantia y Letras 

    top =  df_tribunal_oral[['REGION','TRIBUNAL','ASIENTO','COMUNA','JUECES','TIPO JUZGADO']] 
    jg = df_juzgados_penales[['REGION','TRIBUNAL','ASIENTO','COMUNA','JUECES','TIPO JUZGADO']] 

    df_listado_tribunales = pd.concat([top, jg], join = "inner")

    click.echo('Tranformación ... Cambiando Nombres') 
    df_tribunal_oral['TRIBUNAL'] = df_tribunal_oral['TRIBUNAL'].progress_apply(data.cleandata.cambio_nombre_juzgados)
    df_juzgados_penales['TRIBUNAL'] = df_juzgados_penales['TRIBUNAL'].progress_apply(data.cleandata.cambio_nombre_juzgados)
    df_listado_tribunales['TRIBUNAL'] = df_listado_tribunales['TRIBUNAL'].progress_apply(data.cleandata.cambio_nombre_juzgados)

    data.save_feather(df_provincias, 'generates_DataRegiones', path='data/processed/subdere')
    data.save_feather(df_listado_tribunales, 'generates_ListadoTribunales', path='data/processed/pjud')
    data.save_feather(df_tribunal_oral, 'generates_TribunalesOrales', path='data/processed/pjud')
    data.save_feather(df_juzgados_penales, 'generates_JuzgadosPenales', path='data/processed/pjud')
    click.echo('Generado archivo Feather. Proceso Terminado')