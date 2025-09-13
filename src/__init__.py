"""
"""

from pathlib import Path


BASE = Path().parent.absolute()
DATA = BASE / "data"
RAW = DATA / "raw"
PROCESSED = DATA / "processed"

DATA.mkdir(exist_ok=True)
RAW.mkdir(exist_ok=True)
PROCESSED.mkdir(exist_ok=True)

if __name__ == '__main__':
    pass