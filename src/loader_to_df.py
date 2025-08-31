"""
A module for loading data into a pandas DataFrame.

This module provides a class to load data from CSV or Excel files into a
pandas DataFrame, handling different file types and potential errors.
"""
import pandas as pd
import logging
from my_common_utils.logger_setup import Logger
from pathlib import Path

logger = logging.getLogger(__name__)

class Loader:
    """
    A class to load data from CSV or Excel files into a DataFrame.
    """
    def __init__(self, source_file: Path):
        """Initializes the Loader with a source file path."""
        self.source_file = source_file
        self.df = pd.DataFrame()
    
    def load_csv_to_df(self):
        """Loads a CSV file into a DataFrame."""
        self.df = pd.read_csv(self.source_file, encoding='utf-8-sig')
        logger.info(f"CSV file '{self.source_file}' successfully loaded into DataFrame.")
        return self.df

    
    def load_excel_to_df(self, sheet_name=0):
        """Loads an Excel file into a DataFrame."""
        self.df = pd.read_excel(self.source_file, sheet_name=sheet_name)
        logger.info(f"Excel file '{self.source_file}' successfully loaded into DataFrame.")
        return self.df

    
    def get_dataframe(self):
        """
        Determines the file type and loads it into a DataFrame.

        Returns:
            pd.DataFrame: The loaded DataFrame.
        """
        self.ext = self.source_file.suffix.lower()
        if self.ext:
            try:
                if self.ext == '.csv':
                    return self.load_csv_to_df()
                elif self.ext in ['.xls', '.xlsx']:
                    return self.load_excel_to_df()
                else:
                    logger.error(f"Unsupported file format: '{self.ext}'. Supported formats are .csv, .xls, .xlsx.")
                    raise ValueError(f"Unsupported file format: '{self.ext}'")
            except FileNotFoundError:
                logger.error(f"Error: The file '{self.source_file}' was not found.")
            except pd.errors.EmptyDataError:
                logger.error(f"Error: The file '{self.source_file}' is empty.")
            except Exception as e:
                logger.error(f"An unexpected error occurred while loading the file '{self.source_file}': {e}", exc_info=True)
            raise
        else:
            logger.error("Error: The source file has no extension.")
            raise ValueError("The source file has no extension.")
        


            
if __name__ == "__main__":
    from config import config
    Logger()
    SOURCE_FILE = config.SOURCE_FILE
    try:
        loader = Loader(SOURCE_FILE)
        df = loader.get_dataframe()
        if df is not None and not df.empty:
            logger.info(f"DataFrame created with {len(df)} rows.")
            logger.info(f"DataFrame head:\n{df.head()}")
        else:
            logger.warning("The DataFrame is empty.")
    except Exception as e:
        logger.critical(f"Process terminated due to an error: {e}", exc_info=True)