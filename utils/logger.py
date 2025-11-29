"""
logger.py
---------------------
Advanced structured logger for ETL pipelines.
- Supports multiple levels: INFO, WARNING, ERROR
- Logs events in JSON format
- Writes to console and file
- Supports metadata for detailed audit and lineage
- Suitable for Bronze, Silver, Gold layers

Author: Cauam Pavonne
"""

import logging
import json
import os
import datetime

# -------------------------------
# Configuration
# -------------------------------
LOG_DIR = "./logs/"
LOG_FILE = os.path.join(LOG_DIR, "pipeline.log")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_LEVEL = logging.INFO

# -------------------------------
# Logger Setup
# -------------------------------
logger = logging.getLogger("ETLLogger")
logger.setLevel(LOG_LEVEL)
logger.propagate = False

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(LOG_LEVEL)

# File handler
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setLevel(LOG_LEVEL)

# JSON Formatter
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        if hasattr(record, "metadata") and record.metadata:
            log_record["metadata"] = record.metadata
        return json.dumps(log_record)

formatter = JsonFormatter()
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Add handlers to logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# -------------------------------
# Logging Functions
# -------------------------------

def log_event(event_type: str, message: str, metadata: dict = None, level: str = "INFO"):
    """
    Logs structured event with optional metadata.
    
    Args:
        event_type: str, e.g., 'bronze_read', 'silver_transform'
        message: str, descriptive message
        metadata: dict, optional additional info
        level: str, log level ('INFO', 'WARNING', 'ERROR')
    """
    extra = {"metadata": {"event_type": event_type}}
    if metadata:
        extra["metadata"].update(metadata)
    
    if level.upper() == "INFO":
        logger.info(message, extra=extra)
    elif level.upper() == "WARNING":
        logger.warning(message, extra=extra)
    elif level.upper() == "ERROR":
        logger.error(message, extra=extra)
    else:
        logger.info(message, extra=extra)

# -------------------------------
# Example Usage
# -------------------------------
if __name__ == "__main__":
    log_event("test_event", "This is an info log example")
    log_event("test_warning", "This is a warning log", {"row_count": 100}, level="WARNING")
    log_event("test_error", "This is an error log", {"error": "Connection failed"}, level="ERROR")
