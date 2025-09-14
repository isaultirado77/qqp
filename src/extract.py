"""
Modulo para extraer los archivos descargados de QQP. 
"""

from src.config import BASE, DATA, RAW
from src.utils.loggin_config import get_logger

import time
import datetime
import shutil
import re
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat

import rarfile
import pandas as pd

# loggin config
logger = get_logger(__name__)

COLUMNS = {
    'producto': str,
    'presentacion': str,
    'marca': str,
    'categoria': 'category',
    'catalogo': 'category',
    'precio': float,
    'fecha_registro': pd.to_datetime,
    'cadena_comercial': str,
    'giro': 'category',
    'nombre_comercial': str,
    'direccion': str,
    'estado': 'category',
    'municipio': 'category',
    'latitud': float,
    'longitud': float,
}

def get_year_from_string(string: str):
    pattern = r"\b(20\d{2})\b"
    m = re.search(pattern, str(string))
    return m.group(1) if m else None

def find_rar_files(years=[], base_path=RAW): 
    rars = [base_path.joinpath(f"QQP_{y}.rar") for y in years]
    existing = {y: p for y, p in zip(years, rars) if p.exists()}
    missing = {y: p for y, p in zip(years, rars) if not p.exists()}

    for k, v in missing.items(): 
        logger.warning(f'{v.relative_to(BASE)} missing, needs to download.\n')
    return existing, missing

def find_unzipped_dirs(years=[], base_path=RAW):
    dirs = [base_path.joinpath(f"QQP_{y}") for y in years]
    existing = {y: p for y, p in zip(years, dirs) if p.exists() and p.is_dir()}
    missing = {y: p for y, p in zip(years, dirs) if not p.exists() or not p.is_dir()}

    for k, v in missing.items():
        logger.warning(f'Directory {v.relative_to(base_path)} missing, need to extract.\n')
    return existing, missing


def extract_rar_file(rar_path: Path, dest_path: Path): 
    try: 
        with rarfile.RarFile(rar_path, mode='r') as archive: 
            archive.extractall(path=str(dest_path))
        logger.info(f'Extracted {rar_path.relative_to(BASE)} -> {dest_path.relative_to(BASE)}\n')
        return True
    
    except rarfile.RarExecError as rex: 
        logger.error(f'Rar exec error for {rar_path.relative_to(BASE)}: {rex}\n')
    
    except Exception as e: 
        logger.error(f'Failed to extract {rar_path.relative_to(BASE)}: {e}\n')

    return False

def _is_empty_dir(path: Path): 
    return not any(path.rglob("*"))

def find_extracted_files(base_path: Path): 
    if not base_path.exists():
        return []
    files = [p for p in base_path.rglob("*") 
             if (p.is_file() and p.suffix.lower() in {'.csv', '.txt'})]
    
    return files

def _csv_chunk_filter_and_append(input_file_path: Path, output_file: Path, chunksize=100_00):
        for chunk in pd.read_csv(
            input_file_path,
            header=None,
            names=COLUMNS.keys(),
            chunksize=chunksize,
            low_memory=True,encoding='latin1', 
            sep=','
            ):
            mask = chunk['estado'].fillna('').str.upper() == 'SONORA'
            filtered = chunk.loc[mask]
            if not filtered.empty: 
                filtered.to_csv(output_file, mode='a', header=True)


def filter_sonora_and_save(input_csv_path: Path, output_dir: Path, year=None):
    output_file_path = output_dir.joinpath(f"QQP_{year}_SON.csv")
    output_file_path.touch(exist_ok=True)

    sufix = input_csv_path.suffix

    try: 
        if sufix in {'.csv', '.txt'}: 
            _ = _csv_chunk_filter_and_append(input_csv_path, output_file_path)
        else: 
            logger.warning(f'Unsopported file type: {sufix}; skipping: {input_csv_path.relative_to(BASE)}\n')
            return None
        
    except Exception:
        logger.exception("Error processing %s", input_csv_path)
        return None


def process_extraction(year, rar_path: Path, max_workers=24): 
    extracted_dir = RAW / f"QQP_{year}"
    extracted_dir.mkdir(parents=True, exist_ok=True)
    
    if _is_empty_dir(extracted_dir): 
        success = extract_rar_file(rar_path, extracted_dir)
        if not success: 
            logger.error(f'Extraction failed for {rar_path.relative_to(BASE)}, skipping year {year}\n')
            return False
        
    files = find_extracted_files(extracted_dir)
    
    output_dir = RAW / f"QQP_{year}_son/"
    output_dir.mkdir(exist_ok=True)

    with ThreadPoolExecutor() as executor: 
        executor.map(filter_sonora_and_save, files, repeat(output_dir), repeat(year))
    logger.info(f' Year: {year}; processed {len(files)} -> {output_dir.relative_to(BASE)}\n')
    return True

def cleanup_year(year, rar_path, base_path=RAW):
    try:
        if rar_path.exists():
            rar_path.unlink()
            logger.info(f'{rar_path.relative_to(BASE)} removed\n')

        extracted_dir = base_path / f"QQP_{year}"
        if extracted_dir.exists():
            shutil.rmtree(extracted_dir)
            logger.info(f'{extracted_dir.relative_to(base_path)} removed.\n')

    except Exception as e:
        logger.exception(f'Cleanup failed for year {year}: {e}\n')

def merge_csv_years(years, base_path=RAW):
    merged_dfs = []
    years_str = "-".join(map(str, years))
    output_file = base_path / f"qqp_{years_str}_sonora.csv"
    output_file.touch()

    for year in years:
        year_dir = base_path / f"QQP_{year}_son"
        if not year_dir.exists():
            logger.warning(f" Directory {year_dir} not found, skipping. ")
            continue

        for csv_file in year_dir.glob("*.csv"):
            try:
                df = pd.read_csv(csv_file, dtype=str)
                merged_dfs.append(df)
                logger.info(f" Added {csv_file.relative_to(base_path)} with {len(df)} rows.")
            except Exception as e:
                logger.error(f" Failed reading {csv_file}: {e}")

    if not merged_dfs:
        logger.warning(" No CSV files to merge. ")
        return None

    merged_df = pd.concat(merged_dfs, ignore_index=True)
    merged_df.to_csv(output_file, index=False)
    logger.info(f" Merged {len(merged_df)} rows -> {output_file.relative_to(base_path)}")

    return output_file


def shoot_parallel_extraction(years, rar_paths, max_workers=4):
    try: 
        with ThreadPoolExecutor(max_workers=max_workers) as executor: 
            executor.map(process_extraction, years, rar_paths)
        return True
    except Exception as e: 
        logger.critical('Cannot process parallel extraction: {e}')

def shoot_parallel_cleaning(years, rar_paths): 
    try: 
        with ThreadPoolExecutor(max_workers=4) as executor: 
            executor.map(cleanup_year, years, rar_paths)
        return True
    except Exception as e: 
        logger.critical('Cannot process parallel extraction: {e}')

def run_extraction(years=[], clean=False, merge_all=False):

    if not years: 
        logger.critical(' No years provided, aborting.\n')
        return
    
    existing_rars, _ = find_rar_files(years, RAW)
    if not existing_rars: 
        logger.critical(' No rar files found, aborting.\n')
        return
    
    start = datetime.datetime.now()
    logger.info(f'Start extraction process at: {start.isoformat()})\n')


    # for year, zipf in existing_rars.items():
    #     sucess = process_extraction(year, zipf)
    sucess = shoot_parallel_extraction(
        existing_rars.keys(), existing_rars.values()
    )

    if clean and sucess:
        shoot_parallel_cleaning(
        existing_rars.keys(), existing_rars.values()
        )        
        
    if merge_all:
        merge_csv_years(years)

    end = datetime.datetime.now()
    logger.info(f'End extraction process at: {end.isoformat()}')


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
        logger.error("No se han especificado años para procesar.")
        logger.info("Para ejecutar el programa, provee al menos un año con la opción '-y', por ejemplo:")
        logger.info("    python script.py -y 2015 2016")
        logger.info("Opciones disponibles:")
        parser.print_help()

