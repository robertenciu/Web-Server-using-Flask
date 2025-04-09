import logging
from logging.handlers import RotatingFileHandler
import time
import os

class UTCFormatter(logging.Formatter):
    converter = time.gmtime  # Use UTC for timestamps

    def formatTime(self, record, datefmt=None):
        return super().formatTime(record, datefmt)

def setup_logger(log_file='webserver.log'):
    if not os.path.exists("logs"):
        os.makedirs("logs")

    logger = logging.getLogger("webserver_logger")
    logger.setLevel(logging.INFO)

    handler = RotatingFileHandler(
        filename=os.path.join("logs", log_file),
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=5
    )

    formatter = UTCFormatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
                             datefmt="%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(handler)

    return logger
