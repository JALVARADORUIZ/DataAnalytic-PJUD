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

def processing_duracion(path_interim = "data/interim/pjud", path_processed = "data/processed/pjud"):
    tqdm.pandas()

    df_duracion = pd.read_feather(f"{path_interim}/Duraciones.feather")

    filtro_fecha = df_duracion[df_duracion['FECHA INGRESO']<='2014-12-31']
    df_duracion.drop(filtro_fecha.index, axis=0, inplace=True)

    filtro_null = df_duracion[df_duracion['FECHA INGRESO'].isnull()]
    df_duracion.drop(filtro_null.index, axis=0, inplace=True)

    click.echo('Normalizando nombres ...')
    df_duracion['TRIBUNAL'] = df_duracion['TRIBUNAL'].progress_apply(data.cleandata.cambio_nombre_juzgados)

    data.save_feather(df_duracion, 'Duraciones', path_processed)
    click.echo('Generado archivo Feather. Proceso Terminado')

def processing_data_cortes(path_pjud = 'data/processed/pjud', path_censo = 'data/processed/censo'):
    tqdm.pandas()

    df_regiones = pd.read_feather(f"{path_pjud}/TerminoRol.feather")
    df_tribunales = pd.read_feather(f"{path_pjud}/ListadoTribunales.feather")
    df_censo = pd.read_feather(f"{path_censo}/Censo2017.feather")
    df_dotacion = pd.read_feather(f"{path_pjud}/ListadoTribunales.feather")

    # Extraigo Cortes de Apelaciones asociadas a Juzgados desde este DataSet
    data_cortes_apelaciones = pd.unique(df_regiones[['CORTE','TRIBUNAL']].values.ravel())
    cortes_tribunales = []

    for datacorte in range(len(data_cortes_apelaciones)):  
        if not data_cortes_apelaciones[datacorte].find('C.A.') == -1:
            corte_apelacion = data_cortes_apelaciones[datacorte]
        else:
            tribunal = data_cortes_apelaciones[datacorte]
            if tribunal.find("TRIBUNAL") != -1:
                separa_ciudad = tribunal.split("PENAL ")
            else:
                separa_ciudad = tribunal.split("GARANTIA ")
            ciudad = separa_ciudad[1]
            cortes_tribunales.append([corte_apelacion,ciudad])
    
    lista_cortes = []

    for trib in df_tribunales.index:
        for indice in range(len(cortes_tribunales)):
            if df_tribunales['ASIENTO'][trib] == cortes_tribunales[indice][1]:
                corte = cortes_tribunales[indice][0]
                lista_cortes.append(corte)
                break

    df_tribunales['CORTE'] = lista_cortes

    poblacion = []

    for trib in df_tribunales.index:
        for indice in df_censo.index:
            if df_tribunales['COMUNA'][trib] == df_censo['NOMBRE COMUNA'][indice]:
                censado = df_censo['TOTAL POBLACIÓN EFECTIVAMENTE CENSADA'][indice]
                hombres = df_censo['HOMBRES '][indice]
                mujeres = df_censo['MUJERES'][indice]
                urbana = df_censo['TOTAL ÁREA URBANA'][indice]
                rural = df_censo['TOTAL ÁREA RURAL'][indice]
                poblacion.append([censado, hombres, mujeres, urbana, rural])
                break

    df_poblacion = pd.DataFrame(poblacion, columns = ['POBLACION', 'HOMBRES', 'MUJERES', 'URBANO', 'RURAL'])

    df_tribunales_poblacion = pd.concat([df_tribunales, df_poblacion.reindex(df_tribunales.index)], axis=1)

    # Creo un dataset con informacion de poblacion que abarca cada tribunal !!!
    tribunales = df_tribunales['TRIBUNAL'].unique()
    poblacion_tribunal = []

    for trib in range(len(tribunales)):  
        poblacion = 0
        hombres = 0
        mujeres = 0
        urbana = 0
        rural = 0
        comunas = []
        for indice in df_tribunales_poblacion.index:
            if tribunales[trib] == df_tribunales_poblacion['TRIBUNAL'][indice]:
                region = df_tribunales_poblacion['REGION'][indice]
                corte = df_tribunales_poblacion['CORTE'][indice]
                tribunal = df_tribunales_poblacion['TRIBUNAL'][indice]
                poblacion = int(df_tribunales_poblacion['POBLACION'][indice]) + poblacion
                hombres = int(df_tribunales_poblacion['HOMBRES'][indice]) + hombres
                mujeres = int(df_tribunales_poblacion['MUJERES'][indice]) + mujeres
                urbana = int(df_tribunales_poblacion['URBANO'][indice]) + urbana
                rural = int(df_tribunales_poblacion['RURAL'][indice]) + rural
                comunas.append(df_tribunales_poblacion['COMUNA'][indice])
                
        poblacion_tribunal.append([region, corte, tribunal, poblacion, hombres, mujeres, urbana, rural, comunas])
        
        
    columnas = ['REGION', 'CORTE', 'TRIBUNAL', 'POBLACION', 'HOMBRES', 'MUJERES', 'URBANO', 'RURAL', 'COMUNAS']
    df_poblacion_jurisdiccion = pd.DataFrame(poblacion_tribunal, columns = columnas)

    # Agregare data de cantidad de jueces a esta Data y Asientio de cada Tribunal.

    dotacion = []
    for indice in df_poblacion_jurisdiccion.index:
        for trib in df_dotacion.index:
            if df_poblacion_jurisdiccion['TRIBUNAL'][indice] == df_dotacion['TRIBUNAL'][trib]:
                jueces = df_dotacion['JUECES'][trib]
                asiento = df_dotacion['ASIENTO'][trib]
                tipo = df_dotacion['TIPO JUZGADO'][trib]
                row = [jueces, asiento, tipo]
                dotacion.append(row)
                break
    columnas = ['JUECES','ASIENTO','TIPO JUZGADO']
    df_anexo = pd.DataFrame(dotacion, columns=columnas)

    df_poblacion_jurisdiccion = pd.concat([df_poblacion_jurisdiccion,df_anexo], axis=1)

    data.save_feather(df_tribunales, 'ListadoTribunalesyCortes', path_pjud)
    data.save_feather(df_tribunales_poblacion, 'DataConsolidada_Poblacion_Tribunales', path_pjud)
    data.save_feather(df_poblacion_jurisdiccion, 'DataConsolidada_Poblacion_Jurisdiccion', path_pjud)
    click.echo('Generado archivo Feather. Proceso Terminado')


    
    