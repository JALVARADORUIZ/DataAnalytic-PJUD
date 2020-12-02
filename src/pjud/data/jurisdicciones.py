import re
import os
import pandas as pd
import numpy as np

from tqdm import tqdm
from unicodedata import normalize

from pjud import data


def garantia():
    tqdm.pandas()

    path_raw = "./data/raw/cot"

    # Articulo 16 COT ref: juzgados de garantías

    with open(f'{path_raw}/Juzgados_Garantia.txt', 'r') as file:
        contenido_jg = ''
        for line in file.readlines():
            contenido_jg += line


    regex_jg=r"(?:(?P<Region>^[\w \']+)\:\n\n)|(?P<JG>^[\w. \-]+)\,\scon\s(?P<Jueces>[\w.\-]+)[a-z\-\s\,]+(?P<Competencia>\.|\s[\w. \-\,]+)"
    matches = re.findall(regex_jg, contenido_jg, re.MULTILINE)

    data_jg = []

    for item in range(0, len(matches)):
        if matches[item][0] != '':
            region = matches[item][0].upper()
        else:
            if matches[item][1] != '':
                ciudad = matches[item][1].upper()
                if ciudad.find("JUZGADO") != -1:
                    juzgado = ciudad

                else:
                    juzgado = f"JUZGADO DE GARANTIA {ciudad}"

            if matches[item][2] != '':
                cantidad_jueces = data.cleandata.transforma_numero(matches[item][2])

            if matches[item][3] == '.':
                competencia = ciudad

            else:
                if matches[item][3] != '':
                    competencia = matches[item][3].upper()

            competencia = competencia.replace(" Y ", ",")
            competencia = competencia.replace(" E ", ",")
            competencia = competencia.replace(".", "")

            comunas = competencia.split(",")

            for comuna in comunas:
                data_jg.append([region, juzgado, ciudad, cantidad_jueces, comuna.strip(), 'GARANTIA'])

    df_juzgados_garantia = pd.DataFrame(data_jg,
                                        columns=['REGION', 'TRIBUNAL', 'ASIENTO', 'JUECES', 'COMUNA', 'TIPO JUZGADO'])

    # Elimino tildes de las columnas object

    cols = df_juzgados_garantia.select_dtypes(include = ["object"]).columns
    df_juzgados_garantia[cols] = df_juzgados_garantia[cols].progress_apply(data.cleandata.elimina_tilde)

    data.save_feather(df_juzgados_garantia, 'JuzgadosGarantia')