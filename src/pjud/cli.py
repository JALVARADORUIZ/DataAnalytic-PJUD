import click
from pjud.data import cleandata, jurisdicciones

@click.group()
def main():
    pass


@main.command('cl-ingresos')
def wrapper_carga_limpieza_ingresos_materia():
    cleandata.carga_limpieza_ingresos_materia()



@main.command('org-garantia')
def wrapper_garantia():
    jurisdicciones.garantia()