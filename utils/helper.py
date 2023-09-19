import os
import logging


def get_logger():
    level = os.environ.get('LOG_LEVEL', 'INFO').upper()
    logger = logging.getLogger(__file__)
    logger.setLevel(level)
    handler = logging.FileHandler('/var/log/container_logs.log')
    if (log_format := os.environ.get('LOG_FORMAT')) is not None:
        formatter = logging.Formatter(log_format)
        handler.setFormatter(formatter)
    handler.setLevel(level)
    logger.addHandler(handler)
    return logger
