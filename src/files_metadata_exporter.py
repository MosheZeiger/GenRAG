"""
Metadata Exporter Module.

This module scans a specified directory for files, extracts metadata based on
a predefined naming convention, and exports the details to a CSV file.
"""
import re
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Iterable, Dict, Any
from my_common_utils.logger_setup import Logger

# Initialize the logger
# This creates a singleton instance and sets up the logger

logger = logging.getLogger(__name__)



# The regex pattern to match the file name format
FILE_NAME_PATTERN = re.compile(r'^(\d{8})\s+(.+)$')

def stream_file_paths(root_folder_path: Path) -> Iterable[Path]:
    """
    Recursively yields all file paths from a given directory.

    This function acts as a generator, yielding one path at a time
    to conserve memory.

    Args:
        path (Path): The root directory to scan.

    Yields:
        Path: The next file path found in the directory tree.
    """
    logger.info(f"Starting to scan directory: {root_folder_path}")

    count = 0
    for file_path in root_folder_path.rglob('*'):
        if file_path.is_file():
            count += 1
            yield file_path 
    logger.info(f"Completed scanning. Total files found: {count}")



def get_file_details(file_path: Path):
    """
    Extracts metadata from a file path using a predefined name format.

    Args:
        file_path (Path): The path to the file.

    Returns:
        dict: A dictionary of file details, or None if the format does not match.
    """
    try:
        file_full_name = file_path.name
        file_name = file_path.stem

        match = FILE_NAME_PATTERN.match(file_name)
        if not match:
            logger.warning(f"File name does not match expected format and will be skipped: {file_full_name}")
            return None

        date_str, rest_of_name = match.groups()

        split_name = rest_of_name.split()
        doc_type = split_name[0]
        original_file_name = ' '.join(split_name)

        try:
            created_date = datetime.strptime(date_str, '%Y%m%d').date().strftime('%d/%m/%Y')
        except ValueError:
            logger.error(f"Invalid date format in file name '{file_full_name}'. Skipping.")
            return None

        file_details = {
            "full_file_path_name": str(file_path),
            "path": str(file_path.parent),
            "file_full_name": file_full_name,
            "file_name": file_name,
            "file_extension": file_path.suffix,
            "original_file_name": original_file_name.strip(),
            "created_date": created_date,
            "doc_type": doc_type.strip()
        }
        return file_details
    except Exception as e:
        logger.error(f"An unexpected error occurred while processing file {file_path}: {e}", exc_info=True)
        return None

from typing import Iterator

def stream_file_details(paths_stream: Iterable[Path]) -> Iterator[Dict[str, Any]]:
    """
    Processes a stream of file paths into a pandas DataFrame.

    Args:
        paths_stream: An iterable (like a list or a generator) of Path objects.

    Yields:
        dict: A dictionary containing file metadata for each file.
    """
    for file_path  in paths_stream:
        file_details = get_file_details(file_path )
        if file_details:
            yield file_details
    logger.info("All file details have been processed and get the details.")

def create_dataframe_from_details(details_stream: Iterable[Dict[str, Any]]) -> pd.DataFrame:
    """
    Constructs a pandas DataFrame from an iterable of file detail dictionaries.

    This function efficiently consumes a generator or any iterable of dictionaries
    and creates a DataFrame in a single operation.

    Args:
        details_stream: An iterable where each item is a dictionary
                        of file metadata.

    Returns:
        A pandas DataFrame containing all the collected metadata.
    """
    logger.info("Starting to create DataFrame from file details.")
    df = pd.DataFrame(details_stream)
    logger.info(f"DataFrame created with {len(df)} rows.")
    return df


def save_details_to_csv(df: pd.DataFrame, output_file: str):
    """
    Saves a DataFrame of file details to a CSV file.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        output_file (str): The name of the output CSV file.
    """
    try:
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"File details successfully saved to '{output_file}'.")
    except Exception as e:
        logger.error(f"Error saving to CSV: {e}", exc_info=True)

if __name__ == "__main__":
    from config import config
    Logger()
    # Constants from the config file
    PATH_TO_SCAN = config.PATH_TO_SCAN
    OUTPUT_CSV = config.OUTPUT_CSV
    try:
        paths_generator = stream_file_paths(PATH_TO_SCAN)
        details_generator = stream_file_details(paths_generator)
        files_df = create_dataframe_from_details(details_generator)
        if not files_df.empty:
            logger.info(f"DataFrame head:\n{files_df.head()}")
            save_details_to_csv(files_df, OUTPUT_CSV)
        else:
            logger.warning("No files matching the pattern were found, CSV not created.")
    except Exception as e:
        logger.critical(f"A critical error occurred in the main process: {e}", exc_info=True)