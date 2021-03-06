import click
from pjud.data import cleandata, jurisdicciones, comunal, dataprocessing, transformdata, dataconsolidated

@click.group()
def main():
    pass


@main.command('clean-ingresos-materia')
def wrapper_carga_limpieza_ingresos_materia():
    cleandata.carga_limpieza_ingresos_materia()

@main.command('clean-terminos-materia')
def wrapper_carga_limpieza_terminos_materia():
    cleandata.carga_limpieza_terminos_materia()

@main.command('clean-ingresos-rol')
def wrapper_carga_limpieza_ingresos_rol():
    cleandata.carga_limpieza_ingresos_rol()

@main.command('clean-terminos-rol')
def wrapper_carga_limpieza_terminos_rol():
    cleandata.carga_limpieza_terminos_rol()

@main.command('clean-inventario')
def wrapper_carga_limpieza_inventario():
    cleandata.carga_limpieza_inventario()

@main.command('clean-audiencias')
def wrapper_carga_limpieza_audiencias():
    cleandata.carga_limpieza_audiencias()

@main.command('clean-duraciones')
def wrapper_carga_limpieza_duraciones():
    cleandata.carga_limpieza_duraciones()

@main.command('clean-delitos')
def wrapper_carga_limpieza_delitos():
    cleandata.carga_limpieza_delitos()

@main.command('organic-garantia')
def wrapper_garantia():
    jurisdicciones.garantia()

@main.command('organic-top')
def wrapper_top():
    jurisdicciones.top()

@main.command('organic-jletras')
def wrapper_juzgados_letras():
    jurisdicciones.juzgados_letras()

@main.command('organic-jpenales')
def wrapper_juzgados_penales():
    jurisdicciones.juzgados_penales()

@main.command('create-comunas')
def wrapper_create_comunas():
    comunal.create_comunas()

@main.command('create-censo')
def wrapper_load_data_censo():
    comunal.load_data_censo()

@main.command('process-materia')
def wrapper_processing_materia():
    dataprocessing.processing_materia()

@main.command('process-rol')
def wrapper_processing_rol():
    dataprocessing.processing_rol()

@main.command('process-audiencias')
def wrapper_processing_audiencias():
    dataprocessing.processing_audiencias()

@main.command('process-inventario')
def wrapper_processing_inventario():
    dataprocessing.processing_inventario()

@main.command('process-duracion')
def wrapper_processing_duracion():
    dataprocessing.processing_duracion()

@main.command('process-cortes')
def wrapper_processing_data_cortes():
    dataprocessing.processing_data_cortes()

@main.command('consolidate-materia')
def wrapper_consolidated_materia():
    dataconsolidated.consolidated_materia()

@main.command('consolidate-rol')
def wrapper_consolidated_rol():
    dataconsolidated.consolidated_rol()

@main.command('consolidate-materia-rol')
def wrapper_consolidated_materia_rol():
    dataconsolidated.consolidated_materia_rol()

@main.command('consolidate-fulldata_causa')
def wrapper_consolidated_fulldata_causa():
    dataconsolidated.consolidated_fulldata_causa()

@main.command('consolidate-fulldata_audiencias')
def wrapper_consolidated_fulldata_audiencias():
    dataconsolidated.consolidated_fulldata_audiencias()

@main.command('consolidate-fulldata_inventario')
def wrapper_consolidated_fulldata_inventario():
    dataconsolidated.consolidated_fulldata_inventario()

@main.command('consolidate-fulldata_duracion')
def wrapper_consolidated_fulldata_duracion():
    dataconsolidated.consolidated_fulldata_duracion()