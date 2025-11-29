import logging
import os
import json

def setup_json_logger(log_path: str):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logger = logging.getLogger("pipeline_logger")
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(log_path)
    handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(handler)
    return logger

logger = setup_json_logger("./logs/pipeline.json")

def log_event(event_type: str, message: str, extra: dict = None):
    log_entry = {
        "event_type": event_type,
        "message": message
    }
    if extra:
        log_entry.update(extra)
    logger.info(json.dumps(log_entry))
