"""
Project Configuration Module.

This module loads settings from a JSON file and defines global project paths
and configuration variables for other modules to use.
"""
import json
import os
from pathlib import Path

# Define core project paths
PROJECT_ROOT = Path(__file__).parent.parent
LOGS_DIR = PROJECT_ROOT / 'logs'

# Path to the settings JSON file
settings_path = Path(__file__).parent / 'settings.json'

try:
    # Load settings from the JSON file
    with open(settings_path, 'r', encoding='utf-8') as f:
        settings = json.load(f)
except FileNotFoundError:
    # If the settings file is not found, use an empty dictionary
    settings = {}

# Retrieve nested settings for 'files_metadata_exporter' with defaults
exporter_settings = settings.get("files_metadata_exporter", {})
PATH_TO_SCAN = Path(exporter_settings.get("path_to_scan", PROJECT_ROOT))
OUTPUT_CSV = Path(exporter_settings.get("output_csv", "files_details.csv"))

# Retrieve nested settings for 'loader_to_df' with defaults
loader_settings = settings.get("loader_to_df", {})
SOURCE_FILE = Path(loader_settings.get("source_file", "files_details.csv"))

# Retrieve nested settings for 'data_comparator' with defaults
comparator_settings = settings.get("data_comparator", {})
DF1_SRC_PATH = Path(comparator_settings.get("df1", {}).get("file_path", "files_details.csv"))
DF2_SRC_PATH = Path(comparator_settings.get("df2", {}).get("file_path", "D:/Google Drive/פרטי/0 פרוייקט עדכון גרסה/זמני שהות/מסמכים בית הדין הרבני/אינדקס מסמכי בית הדין.xlsx"))
DF1_ON_COLUMNS = comparator_settings.get("df1", {}).get("on_columns", ["file_name", "parent_folder_name"])
DF2_ON_COLUMNS = comparator_settings.get("df2", {}).get("on_columns", ["שם המסמך", "תיקיית אב"])