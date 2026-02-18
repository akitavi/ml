import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from config import settings

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(settings.LOG_LEVEL)

    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s")

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if settings.LOG_TO_FILE:
        os.makedirs(settings.LOG_DIR, exist_ok=True)
        log_path = os.path.join(settings.LOG_DIR, f"{name}.log")
        file_handler = RotatingFileHandler(log_path, maxBytes=10_000_000, backupCount=2)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger