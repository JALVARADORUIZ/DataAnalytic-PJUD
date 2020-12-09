import os
import pandas as pd
import numpy as np
import click

from tqdm import tqdm
from unicodedata import normalize

from pjud import data

def consolidated_materia(path_processed = "data/processed/pjud"):
    tqdm.pandas()

    df_ingresos_materia = pd.read_feather(f"{path_processed}/IngresosMateria.feather")
    df_termino_materia = pd.read_feather(f"{path_processed}/TerminoMateria.feather")
    df_fulldata_materia = pd.merge(df_ingresos_materia, df_termino_materia, how='outer', on=['COD. TRIBUNAL','RIT','COD. MATERIA'])

    columnas_drop = ['level_0_x', 'index_x', 'level_0_y', 'index_y', 'MES INGRESO', 'MES TERMINO']
    df_fulldata_materia.drop(columnas_drop, axis = 'columns', inplace = True)

    click.echo('Transformando data faltante ...')
    df_fulldata_materia = df_fulldata_materia.progress_apply(data.transformdata.faltantes_materia, axis=1)

    columnas_drop = ['TIPO CAUSA_y', 'MATERIA_y', 'TRIBUNAL_y', 'COD. CORTE_y', 'CORTE_y', 'FECHA INGRESO_y']
    df_fulldata_materia.drop(columnas_drop, axis = 'columns', inplace = True)

    df_fulldata_materia.rename(columns = {'COD. CORTE_x':'COD. CORTE',
                                          'CORTE_x':'CORTE',
                                          'TRIBUNAL_x':'TRIBUNAL',
                                          'TIPO CAUSA_x':'TIPO CAUSA',
                                          'MATERIA_x':'MATERIA',
                                          'FECHA INGRESO_x':'FECHA INGRESO'
                                         }, inplace = True)

    filtro_oral = df_fulldata_materia[df_fulldata_materia['TRIBUNAL'].str.contains('ORAL')]
    filtro_garantia = df_fulldata_materia[df_fulldata_materia['TRIBUNAL'].str.contains('GARANTIA')]

    data.save_feather(df_fulldata_materia, 'Consolidado_Materia', path_processed)
    data.save_feather(filtro_oral, 'JuicioOralesMateria', path_processed)
    data.save_feather(filtro_garantia, 'CausasGarantiaMateria', path_processed)
    click.echo('Generado archivo Feather. Proceso Terminado')

def consolidated_rol(path_processed = "data/processed/pjud"):
    tqdm.pandas()

    df_ingresos_rol = pd.read_feather(f"{path_processed}/IngresosRol.feather")
    df_termino_rol = pd.read_feather(f"{path_processed}/TerminoRol.feather")

    df_fulldata_rol = pd.merge(df_ingresos_rol, df_termino_rol, how='outer', on=['COD. TRIBUNAL','RIT'])

    columnas_drop = ['level_0_x', 'index_x', 'level_0_y', 'index_y', 'MES INGRESO', 'MES TERMINO']
    df_fulldata_rol.drop(columnas_drop, axis = 'columns', inplace = True)

    click.echo('Transformando data faltante ...')
    df_fulldata_rol = df_fulldata_rol.progress_apply(data.transformdata.faltantes_rol, axis=1)

    columnas_drop = ['TIPO CAUSA_y', 'TRIBUNAL_y', 'COD. CORTE_y', 'CORTE_y', 'FECHA INGRESO_y']
    df_fulldata_rol.drop(columnas_drop, axis = 'columns', inplace = True)

    df_fulldata_rol.rename(columns = {'COD. CORTE_x':'COD. CORTE',
                                  'CORTE_x':'CORTE',
                                  'TRIBUNAL_x':'TRIBUNAL',
                                  'TIPO CAUSA_x':'TIPO CAUSA',
                                  'MATERIA_x':'MATERIA',
                                  'FECHA INGRESO_x':'FECHA INGRESO'
                                  }, inplace = True)
    
    causas_top = df_fulldata_rol[df_fulldata_rol['TRIBUNAL'].str.contains('ORAL')]
    causas_garantia = df_fulldata_rol[df_fulldata_rol['TRIBUNAL'].str.contains('GARANTIA')]

    df_rit_cero = df_fulldata_rol[df_fulldata_rol['RIT'].str.startswith("0-")]
    df_fulldata_rol.drop(df_rit_cero.index, axis=0, inplace=True)

    data.save_feather(df_fulldata_rol, 'Consolidado_Rol', path_processed)
    data.save_feather(causas_top, 'JuicioOralesRol', path_processed)
    data.save_feather(causas_garantia, 'CausasGarantiaRol', path_processed)
    click.echo('Generado archivo Feather. Proceso Terminado')

def consolidated_materia_rol(path_processed = "data/processed/pjud"):
    tqdm.pandas()

    df_materia = pd.read_feather(f"{path_processed}/Consolidado_Materia.feather")
    df_rol = pd.read_feather(f"{path_processed}/Consolidado_Rol.feather")

    df_union = pd.merge(df_materia, df_rol, how='outer', on=['COD. CORTE','COD. TRIBUNAL','RIT'])
    
    data.save_feather(df_union, 'Consolidado_Materia_Rol', path_processed)
    click.echo('Generado archivo Feather. Proceso Terminado')

def consolidated_full(path_processed = "data/processed/pjud"):
    tqdm.pandas()

    df_causas = pd.read_feather(f"{path_processed}/Consolidado_Materia_Rol.feather")
    df_audiencias = pd.read_feather(f"{path_processed}/Audiencias.feather")

    df_consolidado = pd.merge(df_causas,df_audiencias, how='outer', on=['COD. TRIBUNAL','RIT'])

    # Unificar RIT y Tribunal en una sola columna para evitar mala interpretacion de causas
    df_consolidado['TRIBUNAL-RIT'] = df_consolidado['COD. TRIBUNAL'].map(str) + "-" + df_consolidado['RIT'].map(str)

    # Carga Data relacionada a Tipologia de delitos
    path_delitos = 'data/processed/delitos'
    df_tipologia = pd.read_feather(f"{path_delitos}/Delitos.feather")
    df_consolidado = pd.merge(df_consolidado,df_tipologia, how='outer', on=['COD. MATERIA'])

    # Carga Data relacionada a Poblacion
    df_poblacion = pd.read_feather(f"{path_processed}/DataConsolidada_Poblacion_Jurisdiccion.feather")
    df_consolidado = pd.merge(df_consolidado,df_poblacion, how='outer', on=['CORTE','TRIBUNAL'])

    columnas_duplicadas = ['index_x', 'index_x','CORTE_x','TRIBUNAL_x','TIPO CAUSA_x','MATERIA_x','FECHA INGRESO_x','AÑO INGRESO_x','FECHA TERMINO_x','AÑO TERMINO_x','MOTIVO TERMINO_x','DURACION CAUSA_x',
                       'TOTAL TERMINOS_x','index_y','CORTE_y','TRIBUNAL_y','TIPO CAUSA_y','level_0','index_y','COD. CORTE_y','index_x','index_y']

    df_consolidado.drop(columnas_duplicadas, axis='columns', inplace=True)

    # Reordenando Nombres de las columnas ...

    df_consolidado.rename(columns = {'COD. CORTE_x':'cod_corte',
                                 'COD. TRIBUNAL':'cod_tribunal',
                                 'RIT':'rit',
                                 'COD. MATERIA':'cod_materia',
                                 'TOTAL INGRESOS POR MATERIAS':'total_ingresos_materia',
                                 'FECHA INGRESO_y':'fecha_ingreso',
                                 'AÑO INGRESO_y':'año_ingreso',
                                 'FECHA TERMINO_y':'fecha_termino',
                                 'DURACION CAUSA_y':'duracion_causa',
                                 'MOTIVO TERMINO_y':'motivo_termino',
                                 'AÑO TERMINO_y':'año_termino',
                                 'TOTAL TERMINOS_y':'total_terminos',
                                 'CORTE':'corte',
                                 'TRIBUNAL':'tribunal',
                                 'TIPO CAUSA':'tipo_causa',
                                 'TIPO DE AUDIENCIA':'tipo_audiencia',
                                 'FECHA PROGRAMACION AUDIENCIA':'fecha_programacion_audiencia',
                                 'FECHA AUDIENCIA':'fecha_audiencia',
                                 'DIAS AGENDAMIENTO':'dias_agendamiento',
                                 'DURACION AUDIENCIA (MIN)':'duracion_audiencia_minutos',
                                 'TOTAL AUDIENCIAS':'total_audiencias',
                                 'TRIBUNAL-RIT':'tribunal_rit',
                                 'MATERIA_y':'materia',
                                 'TIPOLOGIA MATERIA':'tipologia_materia',
                                 'VIGENCIA MATERIA':'vigencia_materia',
                                 'REGION':'region',
                                 'POBLACION':'poblacion',
                                 'HOMBRES':'hombres',
                                 'MUJERES':'mujeres',
                                 'URBANO':'urbano',
                                 'RURAL':'rural',
                                 'COMUNAS':'comunas',
                                 'JUECES':'dotacion_jueces',
                                 'ASIENTO':'asiento',
                                 'TIPO JUZGADO':'tipo_juzgado'
                                 },inplace = True)
    
    df_consolidado = df_consolidado[['region','cod_corte','corte','tribunal_rit','cod_tribunal','rit','tribunal','tipo_juzgado','dotacion_jueces','tipo_causa','fecha_ingreso','año_ingreso','cod_materia','materia',
                                 'tipologia_materia','vigencia_materia','tipo_audiencia','fecha_programacion_audiencia','fecha_audiencia','dias_agendamiento','duracion_audiencia_minutos','total_audiencias',
                                 'total_ingresos_materia','total_terminos','fecha_termino','año_termino','duracion_causa','motivo_termino','asiento','comunas','poblacion','hombres','mujeres','urbano','rural']]

    # Eliminamos registros sin cod_corte ya que son registros incompletos.

    df_consolidado.drop(df_consolidado[df_consolidado['cod_corte'].isnull()].index, inplace=True)

    data.save_feather(df_consolidado, 'Consolidado_FULL', path_processed)
    click.echo('Generado archivo Feather. Proceso Terminado')
