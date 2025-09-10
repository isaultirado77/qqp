"""
Módulo para descargar y cargar datos de la base 'Quién es Quién en los Precios' (Profeco).
"""
import os
import re
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor


import requests
from bs4 import BeautifulSoup

URL_BASE = "https://datos.profeco.gob.mx/datos_abiertos/qqp.php"
URL_DOWNLOAD_ROOT = "https://datos.profeco.gob.mx/datos_abiertos/"
RAW_DATA_PATH = Path().parent.joinpath("data/raw")

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

def download_file(url, path=RAW_DATA_PATH):
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

    if file_path.is_file(): # The file already exists
       return 
     
    # download by chunks
    with open(file_path, mode='wb') as file:
        for chunk in response.iter_content(chunk_size=1024*1024):  # 1 MB
            if chunk:  # avoid empty chunks
                file.write(chunk)
    
    if file_path.is_file(): 
        print('Data downloaded at: ', file_path)
        
    return file_path

def download_files(urls, downloader=download_file): 
    """
    """
    with ThreadPoolExecutor(max_workers=3) as executor: 
        executor.map(downloader, urls)

def check_existing(years, path=RAW_DATA_PATH): 
    """
    """
    missing = []
    for year in years:
        year_folder = path / f'QQP_{year}'
        year_file = path / f'QQP_{year}.rar'
        if  year_folder.exists() or year_file.exists(): 
            print(f'Year {year} already exists.')
        else: 
            missing.append(year)
    return missing

def run_downloader(years=None, path=RAW_DATA_PATH): 
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
        print('Downloading...') 
        download_files(urls)
    else:
        print('All files are already downloaded/unzipped')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Descargar datos de Profeco por año")
    parser.add_argument(
        "-y", "--years", nargs="+", help="Lista de años a descargar (ej: 2021 2022 2023)"
    )
    args = parser.parse_args()
    run_downloader(years=args.years)
