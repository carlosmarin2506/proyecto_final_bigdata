import os
import requests
import gzip
import shutil

URL_BASICS  = "https://datasets.imdbws.com/title.basics.tsv.gz"
URL_RATINGS = "https://datasets.imdbws.com/title.ratings.tsv.gz"

DEST = "datasets"

os.makedirs(DEST, exist_ok=True)

def download_and_extract(url, name):
    gz_path  = f"{DEST}/{name}.tsv.gz"
    tsv_path = f"{DEST}/{name}.tsv"

    print(f"Descargando {name} ...")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(gz_path, "wb") as f:
            shutil.copyfileobj(r.raw, f)

    print(f"Descomprimiendo {name} ...")
    with gzip.open(gz_path, "rb") as f_in:
        with open(tsv_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    os.remove(gz_path)
    print(f"Listo: {tsv_path}")

download_and_extract(URL_BASICS,  "title.basics")
download_and_extract(URL_RATINGS, "title.ratings")

print("\nDatasets descargados correctamente.")
