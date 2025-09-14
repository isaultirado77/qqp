"""
Módulo para descargar y cargar datos de la base 'Quién es Quién en los Precios' (Profeco).
"""

from src.config import BASE, DATA, RAW
from src.utils.loggin_config import get_logger

import time
import datetime
import re
import rarfile
import json
import argparse
from pathlib import Path
from hashlib import md5
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor


import requests
import tqdm
from bs4 import BeautifulSoup


# loggin config
logger = get_logger(__name__)

URL_BASE = "https://datos.profeco.gob.mx/datos_abiertos/qqp.php"
URL_DOWNLOAD_ROOT = "https://datos.profeco.gob.mx/datos_abiertos/"
# URL_TEST = "https://datos.profeco.gob.mx/lol-no-existe"


def get_file_links():
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
    else: 
        logger.critical("Responce error. ")
        return {}
    return dict_links

def check_existing(years, path=RAW):
    missing = []
    existing = []
    existing_files = [f.name for f in path.iterdir()
                      if (f.is_file() and 
                          f.suffix in {'.rar', '.RAR', '.zip', '.ZIP'})]
    
    for y in years: 
        if any(str(y) in name for name in existing_files): 
            logger.info(f'Year {y}.zip already exists in {path.relative_to(BASE)}, skipping...\n')
            existing.append(y)
        else: 
            missing.append(y)
    
    return missing, existing

def is_valid_rar(file_path):
    try:
        with rarfile.RarFile(file_path, "r") as rf:
            rf.testrar()
        return True
    except rarfile.Error as e:
        logger.error(f"Invalid rar file: {file_path} ({e})")
        return False
    
def generate_download_metadata(filename, file_path, url, hash_value, is_valid_rar, download_time):
    metadata_dir = RAW / "metadata_download"
    metadata_dir.mkdir(exist_ok=True) 
    # logger.debug(f'{metadata_dir}: {metadata_dir.exists()}') OK

    metadata = {
        'filename': filename, 
        'path': str(file_path),
        'url': url, 
        'date_downloaded': datetime.now().isoformat(), 
        'hash': hash_value,
        'file_size_actual': file_path.stat().st_size, 
        'valid': is_valid_rar, 
        'elapsed_time': download_time
    }

    # guardar metadatos en data/raw/metadata_download
    metadata_file = metadata_dir / f"{filename}.json"
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=4, ensure_ascii=False)  # OK
    

def download_file(url, path=RAW):
    response = requests.get(url, stream=True) 

    # Get filename
    if "content-disposition" in response.headers:
        content_disposition = response.headers["content-disposition"]
        filename = content_disposition.split("filename=")[-1]
    else:
        filename = url.split("/")[-1]

    file_path = path / filename
    hash_object = md5()
    chunk_size = 10*1024*1024  # 10 MB
    start = time.time()
        
    # download by chunks
    with open(file_path, "wb") as f:
        # print(f"Downloading {filename}: ", end="", flush=True)
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)
                hash_object.update(chunk)
                # print("|", end="", flush=True)  # imprime barra por cada chunk
        print()

    elapsed = time.time() - start

    # Validate file
    if not is_valid_rar(file_path):
            logger.warning(f'Incomplete download for {file_path.name}, retrying...')
            file_path.unlink(missing_ok=True)  # eliminar incompleto
            return download_file(url, path)  # reintento

    _ = generate_download_metadata(
        filename=filename,
        file_path=file_path.relative_to(BASE),
        url=url,
        hash_value=hash_object.hexdigest(),
        is_valid_rar=is_valid_rar(file_path),
        download_time=elapsed
    )

    if file_path.is_file(): 
        logger.info(f"Data downloaded at: {file_path.relative_to(BASE)} in {elapsed:.1}s\n")
        
    return file_path

def download_files(urls, downloader=download_file): 
    with ThreadPoolExecutor(max_workers=3) as executor: 
        executor.map(downloader, urls)

def run_downloader(years=None, path=RAW): 
    """
    Descarga los datos de para los años indicados. 
    Si `years` es None, intenta descargar todos los disponibles. 
    """
    links_dic = get_file_links()
    # for k, v in links.items(): logger.debug(f"{k}: {v}")  # OK
    # if not links: 
    #     logger.debug("LOL, NO LINKS")
    #     return 
    
    if years: 
        selected_years = (year for year in years if year in links_dic)
    # logger.debug(str(selected_years))  # OK

    years_to_download, _ = check_existing(selected_years)
    # logger.debug(years_to_download)  # OK
    
    links = (links_dic[y] for y in years_to_download)
    # for l in links: logger.debug(l)  # OK
    
    if links:
        start = datetime.now()
        logger.info(f'Start download process at: {start.isoformat()})\n') 
        download_files(links)
        end = datetime.now()
        logger.info(f'End download process at: {end.isoformat()}')
    else:
        logger.info('All year files are already downloaded. ')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Descargar datos de Profeco por año")
    parser.add_argument(
        "-y", "--years", nargs="+", help="Lista de años a descargar (ej: 2021 2022 2023)"
    )
    args = parser.parse_args()
    run_downloader(years=args.years)
