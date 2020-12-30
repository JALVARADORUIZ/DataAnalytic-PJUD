import os
import pandas as pd
import numpy as np
import click

from tqdm import tqdm
from unicodedata import normalize

from pjud import data

def consolidated_materia(path_processed = "data/processed/pjud"):
    tqdm.pandas()

    df_ingresos_materia = pd.read_feather(f"{path_processed}/processes_IngresosMateria.feather")
    df_termino_materia = pd.read_feather(f"{path_processed}/processes_TerminosMateria.feather")

    df_fulldata_materia = pd.merge(df_ingresos_materia, df_termino_materia, how='outer', on=['COD. TRIBUNAL','RIT','COD. MATERIA'])

    columnas_drop = ['index_x', 'index_y', 'MES INGRESO', 'MES TERMINO']
    df_fulldata_materia.drop(columnas_drop, axis = 'columns', inplace = True)

    click.echo('Transformando data faltante ...')
    df_fulldata_materia = df_fulldata_materia.progress_apply(data.transformdata.faltantes_materia, axis=1)

    columnas_drop = ['TIPO CAUSA_y', 'MATERIA_y', 'TRIBUNAL_y', 'COD. CORTE_y', 'CORTE_y', 'FECHA INGRESO_y']
    df_fulldata_materia.drop(columnas_drop, axis = 'columns', inplace = True)

    df_fulldata_materia.rename(columns =   {'COD. CORTE_x':'COD. CORTE',
                                            'CORTE_x':'CORTE',
                                            'TRIBUNAL_x':'TRIBUNAL',
                                            'TIPO CAUSA_x':'TIPO CAUSA',
                                            'MATERIA_x':'MATERIA',
                                            'FECHA INGRESO_x':'FECHA INGRESO'
                                            }, inplace = True)

    filtro_oral = df_fulldata_materia[df_fulldata_materia['TRIBUNAL'].str.contains('ORAL')]
    filtro_garantia = df_fulldata_materia[df_fulldata_materia['TRIBUNAL'].str.contains('GARANTIA')]

    data.save_feather(df_fulldata_materia, 'consolidated_Materia', path_processed)
    data.save_feather(filtro_oral, 'consolidated_JuicioOralesMateria', path_processed)
    data.save_feather(filtro_garantia, 'consolidated_CausasGarantiaMateria', path_processed)
    click.echo('Generado archivo Feather. Proceso Terminado')

def consolidated_rol(path_processed = "data/processed/pjud"):
    tqdm.pandas()

    df_ingresos_rol = pd.read_feather(f"{path_processed}/processes_IngresosRol.feather")
    df_termino_rol = pd.read_feather(f"{path_processed}/processes_TerminosRol.feather")

    df_fulldata_rol = pd.merge(df_ingresos_rol, df_termino_rol, how='outer', on=['COD. TRIBUNAL','RIT'])

    columnas_drop = ['index_x', 'index_y', 'MES INGRESO', 'MES TERMINO']
    df_fulldata_rol.drop(columnas_drop, axis = 'columns', inplace = True)

    click.echo('Transformando data faltante ...')
    df_fulldata_rol = df_fulldata_rol.progress_apply(data.transformdata.faltantes_rol, axis=1)

    columnas_drop = ['TIPO CAUSA_y', 'TRIBUNAL_y', 'COD. CORTE_y', 'CORTE_y', 'FECHA INGRESO_y']
    df_fulldata_rol.drop(columnas_drop, axis = 'columns', inplace = True)

    df_fulldata_rol.rename(columns =   {'COD. CORTE_x':'COD. CORTE',
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

    data.save_feather(df_fulldata_rol, 'consolidated_Rol', path_processed)
    data.save_feather(causas_top, 'consolidated_JuicioOralesRol', path_processed)
    data.save_feather(causas_garantia, 'consolidated_CausasGarantiaRol', path_processed)
    click.echo('Generado archivo Feather. Proceso Terminado')

def consolidated_materia_rol(path_processed = "data/processed/pjud"):
    tqdm.pandas()

    df_materia = pd.read_feather(f"{path_processed}/consolidated_Materia.feather")
    df_rol = pd.read_feather(f"{path_processed}/consolidated_Rol.feather")

    df_union = pd.merge(df_rol, df_materia, how='left', on=['COD. CORTE','COD. TRIBUNAL','RIT'], indicator=True)

    columnas_duplicadas = ['index_x', 'index_y','CORTE_y', 'TRIBUNAL_y',
                            'TIPO CAUSA_y', 'FECHA INGRESO_y',
                            'AÑO INGRESO_y', 'FECHA TERMINO_y',
                            'AÑO TERMINO_y', 'MOTIVO TERMINO_y','DURACION CAUSA_y',
                            'TOTAL TERMINOS_y', '_merge']

    df_union.drop(columnas_duplicadas, axis='columns', inplace=True)

    df_union.rename(columns = {'CORTE_x':'CORTE',
                                'TRIBUNAL_x':'TRIBUNAL',
                                'TIPO CAUSA_x':'TIPO CAUSA',
                                'FECHA INGRESO_x':'FECHA INGRESO',
                                'AÑO INGRESO_x':'AÑO INGRESO',
                                'FECHA TERMINO_x':'FECHA TERMINO',
                                'AÑO TERMINO_x':'AÑO TERMINO',
                                'MOTIVO TERMINO_x':'MOTIVO TERMINO',
                                'DURACION CAUSA_x':'DURACION CAUSA',
                                'TOTAL TERMINOS_x':'TOTAL TERMINOS'
                                },inplace = True)
    
    data.save_feather(df_union, 'consolidated_Materia_Rol', path_processed)
    click.echo('Generado archivo Feather. Proceso Terminado')

def consolidated_fulldata_causa(path_processed = "data/processed/pjud"):
    tqdm.pandas()
    path_delitos = 'data/processed/delitos'

    df_causas = pd.read_feather(f"{path_processed}/consolidated_Materia_Rol.feather")
    df_tipologia = pd.read_feather(f"{path_delitos}/clean_Delitos.feather")
    df_poblacion = pd.read_feather(f"{path_processed}/processes_DataConsolidada_Poblacion_Jurisdiccion.feather")
    # Unificar RIT y Tribunal en una sola columna para evitar mala interpretacion de causas
    df_causas['TRIBUNAL-RIT'] = df_causas['COD. TRIBUNAL'].map(str) + "-" + df_causas['RIT'].map(str)

    # Carga Data relacionada a Tipologia de delitos
    df_causa_tipologia = pd.merge(df_causas,df_tipologia, how='left', on=['COD. MATERIA'])

    columnas_duplicadas = ['index_x', 'MATERIA_x','index_y']
    df_causa_tipologia.drop(columnas_duplicadas, axis='columns', inplace=True)

    df_causa_tipologia.rename(columns = {'MATERIA_y':'MATERIA'}, inplace=True)

    # Carga Data relacionada a Poblacion
    
    df_fulldatacausa = pd.merge(df_causa_tipologia, df_poblacion, how='left', on=['CORTE','TRIBUNAL'])

    columnas_duplicadas = ['index']
    df_fulldatacausa.drop(columnas_duplicadas, axis='columns', inplace=True)

    # Reordenando Nombres de las columnas ...
    df_fulldatacausa.rename(columns = { 'COD. CORTE':'cod_corte',
                                        'COD. TRIBUNAL':'cod_tribunal',
                                        'RIT':'rit',
                                        'COD. MATERIA':'cod_materia',
                                        'TOTAL INGRESOS POR MATERIAS':'total_ingresos_materia',
                                        'FECHA INGRESO':'fecha_ingreso',
                                        'AÑO INGRESO':'año_ingreso',
                                        'FECHA TERMINO':'fecha_termino',
                                        'DURACION CAUSA':'duracion_causa',
                                        'MOTIVO TERMINO':'motivo_termino',
                                        'AÑO TERMINO':'año_termino',
                                        'TOTAL TERMINOS':'total_terminos',
                                        'CORTE':'corte',
                                        'TRIBUNAL':'tribunal',
                                        'TIPO CAUSA':'tipo_causa',
                                        'TRIBUNAL-RIT':'tribunal_rit',
                                        'MATERIA':'materia',
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
    
    df_fulldatacausa = df_fulldatacausa[['region','cod_corte','corte','tribunal_rit','cod_tribunal','rit','tribunal','tipo_juzgado','dotacion_jueces','tipo_causa','fecha_ingreso','año_ingreso','cod_materia','materia',
                                        'tipologia_materia','vigencia_materia','total_ingresos_materia','total_terminos','fecha_termino','año_termino','duracion_causa','motivo_termino','asiento','comunas','poblacion',
                                        'hombres','mujeres','urbano','rural']]

    data.save_feather(df_fulldatacausa, 'consolidated_FullData_Causa', path_processed)
    click.echo('Generado archivo Feather. Proceso Terminado')

def consolidated_fulldata_audiencias(path_processed = "data/processed/pjud"):
    tqdm.pandas()

    df_audiencias = pd.read_feather(f"{path_processed}/processes_Audiencias.feather")
    df_poblacion = pd.read_feather(f"{path_processed}/processes_DataConsolidada_Poblacion_Jurisdiccion.feather")


    df_audiencias['TRIBUNAL-RIT'] = df_audiencias['COD. TRIBUNAL'].map(str) + "-" + df_audiencias['RIT'].map(str)

    df_audiencias['AÑO INGRESO'] = df_audiencias['RIT'].progress_apply(data.cleandata.obtiene_año)
    
    columnas_duplicadas = ['level_0', 'index']
    df_audiencias.drop(columnas_duplicadas, axis='columns', inplace=True)

    df_fulldataaudiencias = pd.merge(df_audiencias, df_poblacion, how='left', on=['CORTE','TRIBUNAL'])

    columnas_duplicadas = ['index']
    df_fulldataaudiencias.drop(columnas_duplicadas, axis='columns', inplace=True)

    df_fulldataaudiencias.rename(columns = {'COD. CORTE':'cod_corte',
                                            'COD. TRIBUNAL':'cod_tribunal',
                                            'RIT':'rit',
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
                                            'AÑO INGRESO':'año_ingreso',
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
    
    df_fulldataaudiencias = df_fulldataaudiencias[['region','cod_corte','corte','tribunal_rit','cod_tribunal','rit','tribunal','tipo_juzgado','dotacion_jueces','tipo_causa','año_ingreso',
                                                    'tipo_audiencia','fecha_programacion_audiencia','fecha_audiencia','dias_agendamiento','duracion_audiencia_minutos','total_audiencias',
                                                    'asiento','comunas','poblacion','hombres','mujeres','urbano','rural']]
    
    data.save_feather(df_fulldataaudiencias, 'consolidated_FullData_Audiencias', path_processed)
    click.echo('Generado archivo Feather. Proceso Terminado')

def consolidated_fulldata_inventario(path_processed = "data/processed/pjud"):
    tqdm.pandas()
    path = "data/processed/delitos"

    df_inventario = pd.read_feather(f"{path_processed}/processes_Inventario.feather")
    df_tipologia = pd.read_feather(f"{path}/clean_Delitos.feather")
    df_poblacion = pd.read_feather(f"{path_processed}/processes_DataConsolidada_Poblacion_Jurisdiccion.feather")

    df_inventario['TRIBUNAL-RIT'] = df_inventario['COD. TRIBUNAL'].map(str) + "-" + df_inventario['RIT'].map(str)
    df_inventario['AÑO INGRESO'] = df_inventario['RIT'].progress_apply(data.cleandata.obtiene_año)

    columnas_duplicadas = ['index']
    df_inventario.drop(columnas_duplicadas, axis='columns', inplace=True)

    df_inventario_tipologia = pd.merge(df_inventario,df_tipologia, how='left', on=['COD. MATERIA'])
    columnas_duplicadas = ['index', 'MATERIA_x']
    df_inventario_tipologia.drop(columnas_duplicadas, axis='columns', inplace=True)

    df_inventario_tipologia.rename(columns = {'MATERIA_y':'MATERIA'}, inplace=True)

    df_fulldatainventario = pd.merge(df_inventario_tipologia, df_poblacion, how='left', on=['CORTE','TRIBUNAL'])
    
    columnas_duplicadas = ['index']
    df_fulldatainventario.drop(columnas_duplicadas, axis='columns', inplace=True)

    df_fulldatainventario.rename(columns = {'COD. CORTE':'cod_corte',
                                            'COD. TRIBUNAL':'cod_tribunal',
                                            'RIT':'rit',
                                            'CORTE':'corte',
                                            'TRIBUNAL':'tribunal',
                                            'COMPETENCIA':'competencia',
                                            'TIPO CAUSA':'tipo_causa',
                                            'COD. MATERIA':'cod_materia',
                                            'TIPO ULT. DILIGENCIA':'tipo_ultima_diligencia',
                                            'FECHA ULT. DILIGENCIA':'fecha_ultima_diligencia',
                                            'FECHA INGRESO':'fecha_ingreso',
                                            'AÑO INGRESO':'año_ingreso',
                                            'TOTAL INVENTARIO':'total_inventario',
                                            'TRIBUNAL-RIT':'tribunal_rit',
                                            'MATERIA':'materia',
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
    
    df_fulldatainventario = df_fulldatainventario[['region','cod_corte','corte','tribunal_rit','cod_tribunal','rit','tribunal','competencia','tipo_juzgado','dotacion_jueces','tipo_causa',
                                                    'año_ingreso','fecha_ingreso','cod_materia','materia','tipologia_materia','vigencia_materia','tipo_ultima_diligencia','fecha_ultima_diligencia',
                                                    'total_inventario','asiento','comunas','poblacion','hombres','mujeres','urbano','rural']]

    data.save_feather(df_fulldatainventario, 'consolidated_FullData_Inventario', path_processed)
    click.echo('Generado archivo Feather. Proceso Terminado')

def consolidated_fulldata_duracion(path_processed = "data/processed/pjud"):
    tqdm.pandas()

    df_duracion = pd.read_feather(f"{path_processed}/processes_Duraciones.feather")
    df_poblacion = pd.read_feather(f"{path_processed}/processes_DataConsolidada_Poblacion_Jurisdiccion.feather")

    df_duracion['TRIBUNAL-RIT'] = df_duracion['COD. TRIBUNAL'].map(str) + "-" + df_duracion['RIT'].map(str)

    df_duracion['AÑO INGRESO'] = df_duracion['RIT'].progress_apply(data.cleandata.obtiene_año)
    columnas_duplicadas = ['index']
    df_duracion.drop(columnas_duplicadas, axis='columns', inplace=True)

    df_fulldataduracion = pd.merge(df_duracion, df_poblacion, how='left', on=['CORTE','TRIBUNAL'])

    columnas_duplicadas = ['index']
    df_fulldataduracion.drop(columnas_duplicadas, axis='columns', inplace=True)

    df_fulldataduracion.rename(columns = {  'COD. CORTE':'cod_corte',
                                            'COD. TRIBUNAL':'cod_tribunal',
                                            'RIT':'rit',
                                            'CORTE':'corte',
                                            'TRIBUNAL':'tribunal',
                                            'TIPO CAUSA':'tipo_causa',
                                            'FECHA INGRESO':'fecha_ingreso',
                                            'AÑO INGRESO':'año_ingreso',
                                            'FECHA TERMINO':'fecha_termino',
                                            'MES TERMINO':'mes_termino',
                                            'AÑO TERMINO':'año_termino',
                                            'TOTAL TERMINOS':'total_terminos',
                                            'DURACIÓN CAUSA':'duracion_causa',
                                            'TRIBUNAL-RIT':'tribunal_rit',
                                            'MOTIVO TERMINO':'motivo_termino',
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
    df_fulldataduracion = df_fulldataduracion[['region','cod_corte','corte','tribunal_rit','cod_tribunal','rit','tribunal','tipo_juzgado','dotacion_jueces','tipo_causa',
                                                'año_ingreso','fecha_ingreso','año_termino','mes_termino','fecha_termino','motivo_termino','total_terminos','duracion_causa',
                                                'asiento','comunas','poblacion','hombres','mujeres','urbano','rural']]

    data.save_feather(df_fulldataduracion, 'consolidated_FullData_Duracion', path_processed)
    click.echo('Generado archivo Feather. Proceso Terminado')