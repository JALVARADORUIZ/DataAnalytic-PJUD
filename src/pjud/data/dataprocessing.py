import os
import pandas as pd
import numpy as np
import click

from tqdm import tqdm
from unicodedata import normalize

from pjud import data


def processing_materia(path_interim = "data/interim/pjud", path_processed = "data/processed/pjud"):
    tqdm.pandas()

    df_ingresos_materia = pd.read_feather(f"{path_interim}/IngresosMateria.feather")
    df_termino_materia = pd.read_feather(f"{path_interim}/TerminoMateria.feather")
    
    click.echo('Normalizando nombres ...') 

    df_ingresos_materia['TRIBUNAL'] = df_ingresos_materia['TRIBUNAL'].progress_apply(data.cleandata.cambio_nombre_juzgados)
    df_termino_materia['TRIBUNAL'] = df_termino_materia['TRIBUNAL'].progress_apply(data.cleandata.cambio_nombre_juzgados)

    # CODIGOS no encontrados [13001, -31075, 0, 4011, 4014, 4013, 4010]

    # Caso Codigo 13001 -> LESIONES LEVES   
    # Este codigo debe tratarse como 13036 identificado como: LESIONES LEVES 494 Nº 5 CÓDIGO PENAL
    df_ingresos_materia['COD. MATERIA'] = df_ingresos_materia['COD. MATERIA'].replace(13001,13036)
    df_termino_materia['COD. MATERIA'] = df_termino_materia['COD. MATERIA'].replace(13001,13036)
    
    # Caso Codigo -31075
    filtro_codigo_31075 = df_ingresos_materia[df_ingresos_materia['COD. MATERIA']==-31075]
    df_ingresos_materia.drop(filtro_codigo_31075.index, axis = 0, inplace = True)

    filtro_codigo_31075 = df_termino_materia[df_termino_materia['COD. MATERIA']==31075]
    df_termino_materia.drop(filtro_codigo_31075.index, axis = 0, inplace = True)

    # Caso Codigo 0
    filtro_codigo_0 = df_ingresos_materia[df_ingresos_materia['COD. MATERIA']==0]
    df_ingresos_materia.drop(filtro_codigo_0.index, axis = 0, inplace = True)

    filtro_codigo_0 = df_termino_materia[df_termino_materia['COD. MATERIA']==0]
    df_termino_materia.drop(filtro_codigo_0.index, axis = 0, inplace = True)

    # Codigos 4011, 4014, 4013, 4010
    # codigos No vigentes, mal cargados en sistemas

    filtro_codigo_4011 = df_ingresos_materia[df_ingresos_materia['COD. MATERIA']==4011]
    filtro_codigo_4014 = df_ingresos_materia[df_ingresos_materia['COD. MATERIA']==4014]
    filtro_codigo_4013 = df_ingresos_materia[df_ingresos_materia['COD. MATERIA']==4013]
    filtro_codigo_4010 = df_ingresos_materia[df_ingresos_materia['COD. MATERIA']==4010]
    
    df_ingresos_materia.drop(filtro_codigo_4011.index, axis = 0, inplace = True)
    df_ingresos_materia.drop(filtro_codigo_4014.index, axis = 0, inplace = True)
    df_ingresos_materia.drop(filtro_codigo_4013.index, axis = 0, inplace = True)
    df_ingresos_materia.drop(filtro_codigo_4010.index, axis = 0, inplace = True)
    
    filtro_codigo_4007 = df_termino_materia[df_termino_materia['COD. MATERIA']==4007]
    filtro_codigo_4011 = df_termino_materia[df_termino_materia['COD. MATERIA']==4011]
    filtro_codigo_4014 = df_termino_materia[df_termino_materia['COD. MATERIA']==4014]
    filtro_codigo_4013 = df_termino_materia[df_termino_materia['COD. MATERIA']==4013]
    filtro_codigo_4010 = df_termino_materia[df_termino_materia['COD. MATERIA']==4010]

    df_termino_materia.drop(filtro_codigo_4011.index, axis = 0, inplace = True)
    df_termino_materia.drop(filtro_codigo_4014.index, axis = 0, inplace = True)
    df_termino_materia.drop(filtro_codigo_4013.index, axis = 0, inplace = True)
    df_termino_materia.drop(filtro_codigo_4010.index, axis = 0, inplace = True)
    df_termino_materia.drop(filtro_codigo_4007.index, axis = 0, inplace = True)

    pd.to_datetime(df_termino_materia['FECHA INGRESO'])
    pd.to_datetime(df_termino_materia['FECHA TERMINO'])

    # Elimino las filas con valores nulos
    filtro_null = df_termino_materia[df_termino_materia['FECHA INGRESO'].isnull()]
    df_termino_materia.drop(filtro_null.index, axis = 0, inplace = True)

    # Verifico las causas con Fecha Ingreso anterior a 01-01-2015
    filtro_fechas = df_termino_materia[df_termino_materia['FECHA INGRESO'] <= '2014-12-31']
    df_termino_materia.drop(filtro_fechas.index, axis = 0, inplace = True)

    click.echo('Procesando Error en Fechas ...')
    df_termino_materia = df_termino_materia.progress_apply(data.transformdata.fechas_cambiadas, axis=1)

    data.save_feather(df_ingresos_materia, 'IngresosMateria', path_processed)
    data.save_feather(df_termino_materia, 'TerminoMateria', path_processed)
    click.echo('Generado archivo Feather. Proceso Terminado')

def processing_rol(path_interim = "data/interim/pjud", path_processed = "data/processed/pjud"):
    tqdm.pandas()

    df_ingresos_rol = pd.read_feather(f"{path_interim}/IngresosRol.feather")
    df_termino_rol = pd.read_feather(f"{path_interim}/TerminoRol.feather")

    click.echo('Normalizando nombres...')
    df_ingresos_rol['TRIBUNAL'] = df_ingresos_rol['TRIBUNAL'].progress_apply(data.cleandata.cambio_nombre_juzgados)
    df_termino_rol['TRIBUNAL'] = df_termino_rol['TRIBUNAL'].progress_apply(data.cleandata.cambio_nombre_juzgados)

    filtro_null = df_termino_rol[df_termino_rol['FECHA INGRESO'].isnull()]
    df_termino_rol.drop(filtro_null.index, axis=0, inplace=True)

    click.echo('Procesando Error en fechas ...')
    df_termino_rol = df_termino_rol.progress_apply(data.transformdata.fechas_cambiadas, axis=1)

    data.save_feather(df_ingresos_rol, 'IngresosRol', path_processed)
    data.save_feather(df_termino_rol, 'TerminoRol', path_processed)
    click.echo('Generado archivo Feather. Proceso Terminado')

def processing_audiencias(path_interim = "data/interim/pjud", path_processed = "data/processed/pjud"):
    tqdm.pandas()
    
    df_audiencias = pd.read_feather(f"{path_interim}/Audiencias.feather")

    click.echo('Procesando Fecha de programación ...')
    df_audiencias = df_audiencias.progress_apply(data.transformdata.fecha_programada, axis=1)

    filtro_fecha = df_audiencias[df_audiencias['FECHA PROGRAMACION AUDIENCIA']<='2014-12-31']
    df_audiencias.drop(filtro_fecha.index, axis=0, inplace=True)

    click.echo('Normalizando nombres ...')
    df_audiencias['TRIBUNAL'] = df_audiencias['TRIBUNAL'].progress_apply(data.cleandata.cambio_nombre_juzgados)

    # Verifico si existen audiencias de causas del período anterior a 2015
    años = range(2000,2015)
    fuera_rango = []

    for año in range(len(años)):
        var = "-" + str(años[año])
        fuera_rango.append(df_audiencias[df_audiencias['RIT'].str.contains(var)])
    
    df_eliminar = pd.concat(fuera_rango, axis=0)
    df_audiencias.drop(df_eliminar.index, axis=0, inplace=True)

    data.save_feather(df_audiencias, 'Audiencias', path_processed)
    click.echo('Generado archivo Feather. Proceso Terminado')

def processing_inventario(path_interim = "data/interim/pjud", path_processed = "data/processed/pjud"):
    tqdm.pandas()

    df_inventario = pd.read_feather(f"{path_interim}/Inventario.feather")
    filtro_fecha = df_inventario[df_inventario['FECHA INGRESO']<='2014-12-31']
    df_inventario.drop(filtro_fecha.index, axis=0, inplace=True)

    filtro_null = df_inventario[df_inventario['FECHA INGRESO'].isnull()]
    df_inventario.drop(filtro_null.index, axis=0, inplace=True)

    click.echo('Normalizando nombres ...')
    df_inventario['TRIBUNAL'] = df_inventario['TRIBUNAL'].progress_apply(data.cleandata.cambio_nombre_juzgados)

    # Codigos no encontrados [13001, -31075, 4014, 4013, 4010, 4011, 0]

    df_inventario['COD. MATERIA'] = df_inventario['COD. MATERIA'].replace(13001,13036)
    filtro_codigo_4011 = df_inventario[df_inventario['COD. MATERIA']==4011]
    filtro_codigo_4014 = df_inventario[df_inventario['COD. MATERIA']==4014]
    filtro_codigo_4013 = df_inventario[df_inventario['COD. MATERIA']==4013]
    filtro_codigo_4010 = df_inventario[df_inventario['COD. MATERIA']==4010]
    filtro_codigo_31075 = df_inventario[df_inventario['COD. MATERIA']==31075]
    filtro_codigo_0 = df_inventario[df_inventario['COD. MATERIA']==0]

    df_inventario.drop(filtro_codigo_4011.index, axis = 0, inplace = True)
    df_inventario.drop(filtro_codigo_4014.index, axis = 0, inplace = True)
    df_inventario.drop(filtro_codigo_4013.index, axis = 0, inplace = True)
    df_inventario.drop(filtro_codigo_4010.index, axis = 0, inplace = True)
    df_inventario.drop(filtro_codigo_31075.index, axis = 0, inplace = True)
    df_inventario.drop(filtro_codigo_0.index, axis = 0, inplace = True)

    data.save_feather(df_inventario, 'Inventario', path_processed)
    click.echo('Generado archivo Feather. Proceso Terminado')

