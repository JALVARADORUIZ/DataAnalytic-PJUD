import os

def save_feather(df, filename, path_interim = "data/interim/pjud"):
    df.reset_index(inplace=True)

    # Guardamos dataset como archivo feather
    os.makedirs(path_interim, exist_ok=True)

    df.to_feather(f'{path_interim}/{filename}.feather')