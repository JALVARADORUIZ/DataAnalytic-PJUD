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

@main.command('cl-delitos')
def wrapper_carga_limpieza_delitos():
    cleandata.carga_limpieza_delitos()

@main.command('org-garantia')
def wrapper_garantia():
    jurisdicciones.garantia()

@main.command('org-top')
def wrapper_garantia():
    jurisdicciones.top()

@main.command('org-jletras')
def wrapper_juzgados_letras():
    jurisdicciones.juzgados_letras()