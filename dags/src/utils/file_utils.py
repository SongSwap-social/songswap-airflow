import logging
import os
from json import dumps, loads

logger = logging.getLogger(__name__)

TMP_DIR = "/tmp/"


# Function for reading a JSON file from a specified location
def read_json_file(file_path: str) -> dict:
    """Read JSON data from file"""
    file = _read_file(file_path)
    # Convert bytes to JSON
    json_data = file.decode()
    return loads(json_data)


def save_json_file(file_path: str, json_data: dict):
    """Save a JSON file to a specified location"""
    # Convert JSON to bytes
    json_bytes = dumps(json_data).encode()
    _save_file(file_path, json_bytes)


# Function for reading a file from a specified location
def _read_file(file_path: str):
    logger.info(f"Reading file from {file_path}")
    try:
        with open(file_path, "rb") as f:
            file = f.read()
    except FileNotFoundError:
        logger.error(f"File not found at {file_path}")
        raise
    return file


# Function for saving a file to a specified location
def _save_file(file_path: str, file: bytes):
    dir_path = os.path.dirname(file_path)
    if not os.path.exists(dir_path):
        logger.warning(f"Directory not found at {dir_path}, creating directory.")
        os.makedirs(dir_path, exist_ok=True)

    logger.info(f"Saving file to {file_path}")
    with open(file_path, "wb") as f:
        f.write(file)


# Function for deleting a file from a specified location
def delete_file(file_path: str):
    logger.info(f"Deleting file from {file_path}")
    os.remove(file_path)
