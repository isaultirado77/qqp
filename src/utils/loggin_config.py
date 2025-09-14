"""Loggin config"""

import src.utils.loggin_config as loggin_config
import logging
import colorlog


def get_logger(name):
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s-%(levelname)s-%(name)s-%(message)s",
        log_colors={
            'DEBUG':    'cyan',
            'INFO':     'green',
            'WARNING':  'yellow',
            'ERROR':    'red',
            'CRITICAL': 'bold_red'
        }
    ))

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        logger.addHandler(handler)

    logger.propagate = False
    return logger