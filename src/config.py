from pathlib import Path

# Rutas base
BASE = Path(__file__).resolve().parent.parent
DATA = BASE / "data"
RAW = DATA / "raw"
PROCESSED = DATA / "processed"

def init_paths(): 
    for path in [DATA, RAW, PROCESSED]: 
        path.mkdir(parents=True, exist_ok=True)