Analisis OpenData Pjud
==============================

El Poder Judicial de la República de Chile está constituido por los tribunales nacionales, autónomos e independientes, establecidos por la ley, a los cuales les corresponde la función jurisdiccional, es decir, el conocimiento y resolución de conflictos de relevancia jurídica, cualquiera que sea su naturaleza o calidad de las personas que en ellos intervengan, sin perjuicio de las excepciones constitucionales o legales.

Nuestro proyecto busca analizar los datos de esta institución referente al ámbito Penal, la cual se refiere al conjunto de comportamientos que dan lugar a un hecho ilícito. 

Un delito consiste en un comportamiento culpable y contrario a la ley que conlleva una pena o sanción, la cual se juzga en tribunales penales como son los Juzgados de Garantía, Juzgados de letras y Garantía y los Tribunales Orales, que además se encuentran organizados por territorio y asociados a Cortes de Apelaciones.

Los registros que se analizan se basan en el proyecto de Open Data del Poder Judicial (http://numeros.pjud.cl) donde se encuentran los datos de todos los tribunales del país en el período 2015 al 2019.

Entre las tareas realizadas en este proyecto estan:

-	Extracción de la Data
-	Analisis preliminar de los datos extraidos
-	Limpieza y transformación de la Data
-	Visualización global de los datos mediante diversos criterios.

Project Organization
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.readthedocs.io


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>


## Instalación
```pip install -e .```