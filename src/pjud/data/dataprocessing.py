import os
import pandas as pd
import numpy as np
import click

from tqdm import tqdm
from unicodedata import normalize

from pjud import data


def processing_materia(path_interim = "data/interim/pjud"):
    tqdm.pandas()

    df_ingresos_materia = pd.read_feather(f"{path_interim}/clean_IngresosMateria.feather")
    df_termino_materia = pd.read_feather(f"{path_interim}/clean_TerminosMateria.feather")
    
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
    
    path_processed = "data/processed/pjud"
    data.save_feather(df_ingresos_materia, 'processes_IngresosMateria', path_processed)
    data.save_feather(df_termino_materia, 'processes_TerminosMateria', path_processed)
    click.echo('Generado archivo Feather. Proceso Terminado')

def processing_rol(path_interim = "data/interim/pjud"):
    tqdm.pandas()

    df_ingresos_rol = pd.read_feather(f"{path_interim}/clean_IngresosRol.feather")
    df_termino_rol = pd.read_feather(f"{path_interim}/clean_TerminosRol.feather")

    click.echo('Normalizando nombres...')
    df_ingresos_rol['TRIBUNAL'] = df_ingresos_rol['TRIBUNAL'].progress_apply(data.cleandata.cambio_nombre_juzgados)
    df_termino_rol['TRIBUNAL'] = df_termino_rol['TRIBUNAL'].progress_apply(data.cleandata.cambio_nombre_juzgados)

    filtro_null = df_termino_rol[df_termino_rol['FECHA INGRESO'].isnull()]
    df_termino_rol.drop(filtro_null.index, axis=0, inplace=True)

    filtro_fecha = df_termino_rol[df_termino_rol['FECHA INGRESO'] <='2014-12-31']
    df_termino_rol.drop(filtro_fecha.index, axis = 0, inplace = True)

    click.echo('Procesando Error en fechas ...')
    df_termino_rol = df_termino_rol.progress_apply(data.transformdata.fechas_cambiadas, axis=1)

    path_processed = "data/processed/pjud"
    data.save_feather(df_ingresos_rol, 'processes_IngresosRol', path_processed)
    data.save_feather(df_termino_rol, 'processes_TerminosRol', path_processed)
    click.echo('Generado archivo Feather. Proceso Terminado')

def processing_audiencias(path_interim = "data/interim/pjud"):
    tqdm.pandas()
    
    df_audiencias = pd.read_feather(f"{path_interim}/clean_Audiencias.feather")

    click.echo('Procesando Fecha de programación ...')
    df_audiencias = df_audiencias.progress_apply(data.transformdata.fecha_programada, axis=1)

    #filtro_fecha = df_audiencias[df_audiencias['FECHA PROGRAMACION AUDIENCIA']<='2014-12-31']
    #df_audiencias.drop(filtro_fecha.index, axis=0, inplace=True)

    click.echo('Normalizando nombres ...')
    df_audiencias['TRIBUNAL'] = df_audiencias['TRIBUNAL'].progress_apply(data.cleandata.cambio_nombre_juzgados)

    # Verifico si existen audiencias de causas del período anterior a 2015
    #años = range(2000,2015)
    #fuera_rango = []

    #for año in range(len(años)):
    #    var = "-" + str(años[año])
    #    fuera_rango.append(df_audiencias[df_audiencias['RIT'].str.contains(var)])
    
    #df_eliminar = pd.concat(fuera_rango, axis=0)
    #df_audiencias.drop(df_eliminar.index, axis=0, inplace=True)

    path_processed = "data/processed/pjud"
    data.save_feather(df_audiencias, 'processes_Audiencias', path_processed)
    click.echo('Generado archivo Feather. Proceso Terminado')

def processing_inventario(path_interim = "data/interim/pjud"):
    tqdm.pandas()

    df_inventario = pd.read_feather(f"{path_interim}/clean_Inventario.feather")
    #filtro_fecha = df_inventario[df_inventario['FECHA INGRESO']<='2014-12-31']
    #df_inventario.drop(filtro_fecha.index, axis=0, inplace=True)

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

    path_processed = "data/processed/pjud"
    data.save_feather(df_inventario, 'processes_Inventario', path_processed)
    click.echo('Generado archivo Feather. Proceso Terminado')

def processing_duracion(path_interim = "data/interim/pjud"):
    tqdm.pandas()

    df_duracion = pd.read_feather(f"{path_interim}/clean_Duraciones.feather")

    #filtro_fecha = df_duracion[df_duracion['FECHA INGRESO']<='2014-12-31']
    #df_duracion.drop(filtro_fecha.index, axis=0, inplace=True)

    filtro_null = df_duracion[df_duracion['FECHA INGRESO'].isnull()]
    df_duracion.drop(filtro_null.index, axis=0, inplace=True)

    click.echo('Normalizando nombres ...')
    df_duracion['TRIBUNAL'] = df_duracion['TRIBUNAL'].progress_apply(data.cleandata.cambio_nombre_juzgados)

    path_processed = "data/processed/pjud"
    data.save_feather(df_duracion, 'processes_Duraciones', path_processed)
    click.echo('Generado archivo Feather. Proceso Terminado')

def processing_data_cortes(path_pjud = "data/processed/pjud", path_censo = "data/processed/censo"):
    tqdm.pandas()

    df_regiones = pd.read_feather(f"{path_pjud}/processes_TerminosRol.feather")
    df_tribunales = pd.read_feather(f"{path_pjud}/generates_ListadoTribunales.feather")
    df_censo = pd.read_feather(f"{path_censo}/generates_Censo2017.feather")

    # Extraigo Cortes de Apelaciones asociadas a Juzgados desde este DataSet

    data_cortes_apelaciones = pd.unique(df_regiones[['CORTE','TRIBUNAL']].values.ravel())
    cortes_tribunales = []

    for datacorte in range(len(data_cortes_apelaciones)):  
        if not data_cortes_apelaciones[datacorte].find('C.A.') == -1:
            corte_apelacion = data_cortes_apelaciones[datacorte]
        else:
            datatribunal = data_cortes_apelaciones[datacorte]
            if datatribunal.find("TRIBUNAL") != -1:
                separa_ciudad = datatribunal.split("PENAL ")
            else:
                separa_ciudad = datatribunal.split("GARANTIA ")
            ciudad = separa_ciudad[1]
            cortes_tribunales.append([corte_apelacion,ciudad,datatribunal])

    
    lista_cortes = []
    poblacion = []

    for trib in df_tribunales.index:
        for indice in range(len(cortes_tribunales)):
            if df_tribunales['TRIBUNAL'][trib] == cortes_tribunales[indice][2]:
                corte = cortes_tribunales[indice][0]
                lista_cortes.append(corte)
                break

        for censo in df_censo.index:
            if df_tribunales['COMUNA'][trib] == df_censo['NOMBRE COMUNA'][censo]:
                censado = df_censo['TOTAL POBLACIÓN EFECTIVAMENTE CENSADA'][censo]
                hombres = df_censo['HOMBRES '][censo]
                mujeres = df_censo['MUJERES'][censo]
                urbana = df_censo['TOTAL ÁREA URBANA'][censo]
                rural = df_censo['TOTAL ÁREA RURAL'][censo]
                poblacion.append([censado, hombres, mujeres, urbana, rural])
                break
    
    df_tribunales['CORTE'] = lista_cortes
 
    df_poblacion = pd.DataFrame(poblacion, columns = ['POBLACION', 'HOMBRES', 'MUJERES', 'URBANO', 'RURAL'])
    df_tribunales_poblacion = pd.concat([df_tribunales, df_poblacion.reindex(df_tribunales.index)], axis=1)

    # Creo un dataset con informacion de poblacion que abarca cada tribunal !!!
    tribunales = df_tribunales['TRIBUNAL'].unique()
    poblacion_tribunal = []

    for tri in range(len(tribunales)):  
        poblacion = 0
        hombres = 0
        mujeres = 0
        urbana = 0
        rural = 0
        comunas = []
        for ind in df_tribunales_poblacion.index:
            if tribunales[tri] == df_tribunales_poblacion['TRIBUNAL'][ind]:
                region = df_tribunales_poblacion['REGION'][ind]
                corte = df_tribunales_poblacion['CORTE'][ind]
                tribunal = df_tribunales_poblacion['TRIBUNAL'][ind]
                poblacion = int(df_tribunales_poblacion['POBLACION'][ind]) + poblacion
                hombres = int(df_tribunales_poblacion['HOMBRES'][ind]) + hombres
                mujeres = int(df_tribunales_poblacion['MUJERES'][ind]) + mujeres
                urbana = int(df_tribunales_poblacion['URBANO'][ind]) + urbana
                rural = int(df_tribunales_poblacion['RURAL'][ind]) + rural
                comunas.append(df_tribunales_poblacion['COMUNA'][ind])
                
        poblacion_tribunal.append([region, corte, tribunal, poblacion, hombres, mujeres, urbana, rural, comunas])
        
        
    columnas = ['REGION', 'CORTE', 'TRIBUNAL', 'POBLACION', 'HOMBRES', 'MUJERES', 'URBANO', 'RURAL', 'COMUNAS']
    df_poblacion_jurisdiccion = pd.DataFrame(poblacion_tribunal, columns = columnas)

    # Agregare data de cantidad de jueces a esta Data y Asientio de cada Tribunal.

    dotacion = []
    for i in df_poblacion_jurisdiccion.index:
        for t in df_tribunales.index:
            if df_poblacion_jurisdiccion['TRIBUNAL'][i] == df_tribunales['TRIBUNAL'][t]:
                jueces = df_tribunales['JUECES'][t]
                asiento = df_tribunales['ASIENTO'][t]
                tipo = df_tribunales['TIPO JUZGADO'][t]
                row = [jueces, asiento, tipo]
                dotacion.append(row)
                break
            
    columnas = ['JUECES','ASIENTO','TIPO JUZGADO']
    df_anexo = pd.DataFrame(dotacion, columns=columnas)

    df_poblacion_jurisdiccion = pd.concat([df_poblacion_jurisdiccion,df_anexo], axis=1)

    
    path_processed = "data/processed/pjud"
    data.save_feather(df_tribunales, "processes_ListadoTribunalesyCortes", path_processed)
    data.save_feather(df_tribunales_poblacion, "processes_DataConsolidada_Poblacion_Tribunales", path_processed)
    data.save_feather(df_poblacion_jurisdiccion, "processes_DataConsolidada_Poblacion_Jurisdiccion", path_processed)
    click.echo('Generado archivo Feather. Proceso Terminado')


    
    