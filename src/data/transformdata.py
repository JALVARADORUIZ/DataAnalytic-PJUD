import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta


def fechas_cambiadas(row):
    if row['DURACION CAUSA'] < 0:
        fecha_inicio = row['FECHA TERMINO']
        fecha_termino = row['FECHA INGRESO']
        row['FECHA INGRESO'] = fecha_termino
        row['FECHA TERMINO'] = fecha_inicio
        row['DURACION CAUSA'] = row['DURACION CAUSA']*-1
    return row

def fecha_programada(row):
    if row['FECHA PROGRAMACION AUDIENCIA'] is pd.NaT:
        row['FECHA PROGRAMACION AUDIENCIA'] = row['FECHA AUDIENCIA'] - pd.tseries.offsets.Day(row['DIAS AGENDAMIENTO'])
    return row


def faltantes_materia(row):
    
    # Caso Causas con INGRESO pero sin TERMINO aún
    
    if pd.notnull(row['FECHA INGRESO_x']) and pd.isnull(row['FECHA INGRESO_y']):
        row['FECHA INGRESO_y'] = row['FECHA INGRESO_x']
        row['MATERIA_y'] = row['MATERIA_x']
        row['COD. CORTE_y'] = row['COD. CORTE_x']
        row['CORTE_y'] = row['CORTE_x']
        row['TRIBUNAL_y'] = row['TRIBUNAL_x']
        row['CORTE_y'] = row['CORTE_x']
        row['TIPO CAUSA_y'] = row['TIPO CAUSA_x']
        row['MOTIVO TERMINO'] = 'SIN TERMINO'
        
    # Caso Causas sin INGRESO pero que los datos estan con TERMINO
    
    if pd.isnull(row['FECHA INGRESO_x']) and pd.notnull(row['FECHA INGRESO_y']):
        row['FECHA INGRESO_x'] = row['FECHA INGRESO_y']
        row['MATERIA_x'] = row['MATERIA_y']
        row['COD. CORTE_x'] = row['COD. CORTE_y']
        row['CORTE_x'] = row['CORTE_y']
        row['TRIBUNAL_x'] = row['TRIBUNAL_y']
        row['CORTE_x'] = row['CORTE_y']
        row['TIPO CAUSA_x'] = row['TIPO CAUSA_y']
        #row['MES INGRESO'] = int(row['FECHA INGRESO_y'].strftime("%m"))
        row['AÑO INGRESO'] = int(row['FECHA INGRESO_y'].strftime("%Y"))
        row['MOTIVO TERMINO'] = str(row['MOTIVO TERMINO']).replace(".","")
        
    # Caso Causas con ingreso y termino
    
    if pd.notnull(row['FECHA INGRESO_x']) and pd.notnull(row['FECHA INGRESO_y']):
        row['MOTIVO TERMINO'] = str(row['MOTIVO TERMINO']).replace(".","")
        
    return row
    

def faltantes_rol(row):
    
    # Caso Causas con INGRESO pero sin TERMINO aún
    
    if pd.notnull(row['FECHA INGRESO_x']) and pd.isnull(row['FECHA INGRESO_y']):
        row['FECHA INGRESO_y'] = row['FECHA INGRESO_x']
        row['COD. CORTE_y'] = row['COD. CORTE_x']
        row['CORTE_y'] = row['CORTE_x']
        row['TRIBUNAL_y'] = row['TRIBUNAL_x']
        row['CORTE_y'] = row['CORTE_x']
        row['TIPO CAUSA_y'] = row['TIPO CAUSA_x']
        row['MOTIVO TERMINO'] = 'SIN TERMINO'
        
    # Caso Causas sin INGRESO pero que los datos estan con TERMINO
    
    if pd.isnull(row['FECHA INGRESO_x']) and pd.notnull(row['FECHA INGRESO_y']):
        row['FECHA INGRESO_x'] = row['FECHA INGRESO_y']
        row['COD. CORTE_x'] = row['COD. CORTE_y']
        row['CORTE_x'] = row['CORTE_y']
        row['TRIBUNAL_x'] = row['TRIBUNAL_y']
        row['CORTE_x'] = row['CORTE_y']
        row['TIPO CAUSA_x'] = row['TIPO CAUSA_y']
        row['AÑO INGRESO'] = int(row['FECHA INGRESO_y'].strftime("%Y"))
        row['MOTIVO TERMINO'] = str(row['MOTIVO TERMINO']).replace(".","")
        
    # Caso Causas con ingreso y termino
    
    if pd.notnull(row['FECHA INGRESO_x']) and pd.notnull(row['FECHA INGRESO_y']):
        row['MOTIVO TERMINO'] = str(row['MOTIVO TERMINO']).replace(".","")
        
    return row