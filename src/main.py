"""
"""

from src.config import init_paths, BASE, RAW, PROCESSED
from src.utils.loggin_config import get_logger

logger = get_logger(__name__)

def main(): 
    init_paths()
    logger.info(f"Directory RAW: {RAW.relative_to(BASE)}")
    logger.info(f"Directory PROCESSED: {PROCESSED.relative_to(BASE)}")

if __name__ == '__main__': 
    main()