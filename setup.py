from setuptools import find_packages, setup

setup(
    name='pjud',
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    version='0.1.0',
    description='Este proyecto busca analizar los datos abiertos del Poder Judicial de Chile.',
    author='escuelita-chilota',
    license='MIT',
    entry_points={
        'console_scripts': [
            'pjud=pjud.cli:main',
        ],
    },
)
