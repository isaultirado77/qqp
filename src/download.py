"""
Módulo para descargar y cargar datos de la base 'Quién es Quién en los Precios' (Profeco).
"""

from __init__ import BASE, DATA, RAW

import re
import rarfile
import logging
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor


import requests
from bs4 import BeautifulSoup


# loggin config
logging.basicConfig(
    level=logging.INFO,
    format="\n%(asctime)s - %(levelname)s - [%(filename)s] - %(message)s"
)

URL_BASE = "https://datos.profeco.gob.mx/datos_abiertos/qqp.php"
URL_DOWNLOAD_ROOT = "https://datos.profeco.gob.mx/datos_abiertos/"


def get_file_links():
    """
    """
    dict_links = {} 
    responce = requests.get(URL_BASE)
    if responce.status_code == 200: 
        soup = BeautifulSoup(responce.content, features='html.parser')
        for link in soup.find_all('a', href=True): 
            text = link.get_text(strip=True)
            href = link['href']
            match = re.search(r"\b(20\d{2})\b", text)
            if match: 
                year = match.group(1)
                dict_links[year] = URL_DOWNLOAD_ROOT+"/"+href
    return dict_links

def check_existing(years, path=RAW):
    """
    Checa si existen archivos o carpetas en `path` que contengan el año en su nombre.
    Devuelve una lista con los años que faltan.
    """
    missing = []
    existing_items = [p.name for p in path.iterdir()]

    for year in years:
        if any(str(year) in name for name in existing_items):
            logging.info(f"Year {year} already exists {path.relative_to(path)}.")
        else:
            missing.append(year)

    return missing

def is_valid_rar(file_path):
    try:
        with rarfile.RarFile(file_path, "r") as rf:
            rf.testrar()
        return True
    except rarfile.Error as e:
        logging.warning("Invalid rar file: %s (%s)", file_path, e)
        return False

def download_file(url, path=RAW):
    """
    """
    response = requests.get(url, stream=True) 

    # Get filename
    if "content-disposition" in response.headers:
        content_disposition = response.headers["content-disposition"]
        filename = content_disposition.split("filename=")[-1]
    else:
        filename = url.split("/")[-1]

    file_path = path / filename
     
    # download by chunks
    with open(file_path, mode='wb') as file:
        for chunk in response.iter_content(chunk_size=1024*1024):  # 1 MB
            if chunk:  # avoid empty chunks
                file.write(chunk)

    # Validate file
    if not is_valid_rar(file_path):
            logging.warning(f'Incomplete download for {file_path.name}, retrying...')
            file_path.unlink(missing_ok=True)  # eliminar incompleto
            return download_file(url, path)  # reintento

    
    if file_path.is_file(): 
        logging.info(f"Data downloaded at: {file_path.relative_to(RAW)}")
        
    return file_path

def download_files(urls, downloader=download_file): 
    """
    """
    with ThreadPoolExecutor(max_workers=3) as executor: 
        executor.map(downloader, urls)

def run_downloader(years=None, path=RAW): 
    """
    Descarga los datos de para los años indicados. 
    Si `years` es None, intenta descargar todos los disponibles. 
    """
    links = get_file_links()
    
    if years: 
        selected_years = [year for year in years if year in links]
    
    years_to_download = check_existing(selected_years)
    urls = [links[y] for y in years_to_download]
    
    if urls:
        logging.info('Downloading...') 
        download_files(urls)
    else:
        logging.info('All year files are already downloaded/unzipped/extracted')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Descargar datos de Profeco por año")
    parser.add_argument(
        "-y", "--years", nargs="+", help="Lista de años a descargar (ej: 2021 2022 2023)"
    )
    args = parser.parse_args()
    run_downloader(years=args.years)
