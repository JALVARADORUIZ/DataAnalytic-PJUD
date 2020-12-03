import click
from pjud.data import cleandata, jurisdicciones

@click.group()
def main():
    pass


@main.command('cl-ingresos-materia')
def wrapper_carga_limpieza_ingresos_materia():
    cleandata.carga_limpieza_ingresos_materia()

@main.command('cl-terminos-materia')
def wrapper_carga_limpieza_terminos_materia():
    cleandata.carga_limpieza_terminos_materia()

@main.command('cl-ingresos-rol')
def wrapper_carga_limpieza_ingresos_rol():
    cleandata.carga_limpieza_ingresos_rol()

@main.command('cl-terminos-rol')
def wrapper_carga_limpieza_terminos_rol():
    cleandata.carga_limpieza_terminos_rol()

@main.command('cl-inventario')
def wrapper_carga_limpieza_inventario():
    cleandata.carga_limpieza_inventario()

@main.command('cl-audiencias')
def wrapper_carga_limpieza_audiencias():
    cleandata.carga_limpieza_audiencias()

@main.command('cl-duraciones')
def wrapper_carga_limpieza_duraciones():
    cleandata.carga_limpieza_duraciones()

@main.command('org-garantia')
def wrapper_garantia():
    jurisdicciones.garantia()