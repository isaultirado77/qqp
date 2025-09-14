"""
Modulo para extraer los archivos descargados de QQP. 
"""

from __init__ import BASE, DATA, RAW

import shutil
import re
import logging
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import repeat

import rarfile
import pandas as pd

# loggin config
logging.basicConfig(
    level=logging.INFO,
    format="\n%(asctime)s - %(levelname)s - [%(filename)s] - %(message)s"
)

COLUMN_NAMES = [
    "producto",
    "presentacion",
    "marca",
    "categoria",
    "catalogo",
    "precio",
    "fecha_registro",
    "cadena_comercial",
    "giro",
    "nombre_comercial",
    "direccion",
    "estado",
    "municipio",
    "latitud",
    "longitud",
]


def get_year(string: str):
    pattern = r"\b(20\d{2})\b"
    m = re.search(pattern, str(string))
    return m.group(1) if m else None

def find_rar_files(years=[], base_path=RAW): 
    rars = [base_path.joinpath(f"QQP_{y}.rar") for y in years]
    existing = {y: p for y, p in zip(years, rars) if p.exists()}
    missing = {y: p for y, p in zip(years, rars) if not p.exists()}

    for k, v in missing.items(): 
        logging.warning(f'{v.relative_to(base_path)} missing, needs to download.')
    return existing, missing

def find_unzipped_dirs(years=[], base_path=RAW):
    dirs = [base_path.joinpath(f"QQP_{y}") for y in years]
    existing = {y: p for y, p in zip(years, dirs) if p.exists() and p.is_dir()}
    missing = {y: p for y, p in zip(years, dirs) if not p.exists() or not p.is_dir()}

    for k, v in missing.items():
        logging.warning(f'Directory {v.relative_to(base_path)} missing, needs extraction.')
    return existing, missing


def extract_rar_file(rar_path: Path, dest_path: Path): 
    try: 
        with rarfile.RarFile(rar_path, mode='r') as archive: 
            archive.extractall(path=str(dest_path))
        logging.info(f'Extracted {rar_path.relative_to(RAW)} -> {dest_path.relative_to(RAW)}')
        return True
    except rarfile.RarExecError as rex: 
        logging.error(f'Rar exec error for {rar_path.relative_to(RAW)}: {rex}')
    except Exception as e: 
        logging.error(f'Failed to extract {rar_path.relative_to(RAW)}: {e}')
    return False

def _is_empty_dir(path: Path): 
    return not any(path.rglob("*"))

def find_extracted_files(base_path: Path): 
    if not base_path.exists():
        return []
    files = [p for p in base_path.rglob("*") if p.is_file() and p.suffix.lower() in {'.csv', '.txt'}]
    return files

def _csv_chunk_filter_and_append(csv_input_path: Path, output_dir: Path, chunksize=100_00):
        filename = csv_input_path.stem + '_son.csv'
        for chunk in pd.read_csv(
            csv_input_path,
            header=None,
            names=COLUMN_NAMES,
            chunksize=chunksize,
            dtype=str,
            low_memory=True,encoding="latin1", 
            sep=','
            ):
            mask = chunk['estado'].fillna('').str.upper() == 'SONORA'
            filtered = chunk.loc[mask]
            if not filtered.empty: 
                filtered.to_csv(output_dir / filename, mode='a', header=True)


def filter_sonora_and_save(csv_input_path: Path, output_dir: Path):
    sufix = csv_input_path.suffix
    try: 
        if sufix in {'.csv', '.txt'}: 
            _ = _csv_chunk_filter_and_append(csv_input_path, output_dir)
        else: 
            logging.warning(f'Unsopported file type: {sufix}; skipping: {csv_input_path.relative_to(RAW)}')
            return None
        
    except Exception:
        logging.exception("Error processing %s", csv_input_path)
        return None


def process_extraction(year, rar_path: Path, max_workers=20): 
    extracted_dir = RAW / f"QQP_{year}"
    extracted_dir.mkdir(parents=True, exist_ok=True)
    
    if _is_empty_dir(extracted_dir): 
        success = extract_rar_file(rar_path, extracted_dir)
        if not success: 
            logging.error(f'Extraction failed for {rar_path.relative_to(RAW)}, skipping year {year}')
            return
        
    files = find_extracted_files(extracted_dir)
    output_dir = RAW / f"QQP_{year}_son/"
    output_dir.mkdir(exist_ok=True)
    with ThreadPoolExecutor() as executor: 
        executor.map(filter_sonora_and_save, files, repeat(output_dir))
    logging.info(f' Year: {year}; processed {len(files)} -> {output_dir.relative_to(RAW)}')
    return True

def cleanup_year(year, rar_path, base_path=RAW):
    try:
        if rar_path.exists():
            rar_path.unlink()
            logging.info(f'{rar_path.relative_to(base_path)} removed.')

        extracted_dir = base_path / f"QQP_{year}"
        if extracted_dir.exists():
            shutil.rmtree(extracted_dir)
            logging.info(f'{extracted_dir.relative_to(base_path)} removed.')
    except Exception as e:
        logging.exception(f'Cleanup failed for year {year}: {e}')

def merge_csv_years(years, base_path=RAW):
    merged_dfs = []
    years_str = "-".join(map(str, years))
    output_file = base_path / f"qqp_{years_str}_sonora.csv"

    for year in years:
        year_dir = base_path / f"QQP_{year}_son"
        if not year_dir.exists():
            logging.warning(f" Directory {year_dir} not found, skipping. ")
            continue

        for csv_file in year_dir.glob("*.csv"):
            try:
                df = pd.read_csv(csv_file, dtype=str)
                merged_dfs.append(df)
                logging.info(f" Added {csv_file.relative_to(base_path)} with {len(df)} rows.")
            except Exception as e:
                logging.error(f" Failed reading {csv_file}: {e}")

    if not merged_dfs:
        logging.warning(" No CSV files to merge. ")
        return None

    merged_df = pd.concat(merged_dfs, ignore_index=True)
    merged_df.to_csv(output_file, index=False)
    logging.info(f" Merged {len(merged_df)} rows -> {output_file.relative_to(base_path)}")

    return output_file


def run_extraction(years=[], clean=False, merge_all=False):

    if not years: 
        logging.warning(' No years provided, aborting. ')
        return
    
    existing_rars, _ = find_rar_files(years, RAW)
    if not existing_rars: 
        logging.warning(' No rar files found, aborting. ')
        return
    
    for year, rar_path in existing_rars.items(): 
        sucess = process_extraction(year, rar_path)

        if clean and sucess:
            _ = cleanup_year(year, rar_path, RAW)
    
    if merge_all: 
        merge_csv_years(years)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Extrae y opcionalmente limpia y fusiona archivos RAR por año.")
    
    parser.add_argument(
        '-y', '--years',
        nargs='+',
        type=int,
        help='Lista de años a procesar (ej.: -y 2015 2016 2017)'
    )
    
    parser.add_argument(
        '-c', '--clean',
        action='store_true',
        help='Limpia los archivos extraídos después de procesarlos'
    )
    
    parser.add_argument(
        '-ma', '--merge-all',
        action='store_true',
        help='Fusiona todos los archivos CSV después de la extracción'
    )

    parser.add_argument(
        '-m', '--merge',
        nargs='+',
        type=int,
        help='Fusiona solo los CSV de los años especificados (ej.: -m 2015 2016)'
    )
    
    args = parser.parse_args()
    
    if args.merge:
        merge_csv_years(args.merge)
    elif args.years:
        run_extraction(years=args.years, clean=args.clean, merge_all=args.merge_all)
    else:
        parser.print_help()
