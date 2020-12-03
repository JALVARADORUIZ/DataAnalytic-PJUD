import os

def save_feather(df, filename, path = "data/interim/pjud"):
    df.reset_index(inplace=True)

    # Guardamos dataset como archivo feather
    os.makedirs(path, exist_ok=True)

    df.to_feather(f'{path}/{filename}.feather')