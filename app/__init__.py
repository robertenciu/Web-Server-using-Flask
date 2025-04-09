import os
from flask import Flask
from app.data_ingestor import DataIngestor
from app.task_runner import ThreadPool
from app.logger_config import setup_logger

if not os.path.exists('results'):
    os.mkdir('results')

# Flask framework
webserver = Flask(__name__)

# Logging setup
webserver.logger = setup_logger()

# Data ingestor
webserver.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")

# Initiating Thread pool
webserver.tasks_runner = ThreadPool(data_ingestor=webserver.data_ingestor)
webserver.tasks_runner.start()

# Initiating job counter
webserver.job_counter = 1

from app import routes
