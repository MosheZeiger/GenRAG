import pandas as pd
import logging
from my_common_utils.logger_setup import Logger
from pathlib import Path

logger = logging.getLogger(__name__)

class Exporter:
    """
    A class to export DataFrames to various formats.
    """
    def __init__(self, df: pd.DataFrame, export_path: Path):
        """Initializes the Exporter with a DataFrame and export path."""
        self.df = df
        self.export_path = export_path
        self.ext = self.export_path.suffix.lower()

    def export_to_csv(self):
        """Exports the DataFrame to a CSV file."""
        self.df.to_csv(self.export_path, index=False, encoding='utf-8-sig')

    def export_to_excel(self):
        """Exports the DataFrame to an Excel file."""
        self.df.to_excel(self.export_path, index=False, engine='openpyxl')
    
    def run_export(self):
        logger.info(f"Starting export to {self.export_path}")
        try:
            if self.ext == '.csv':
                self.export_to_csv()
            elif self.ext == '.xlsx':
                self.export_to_excel()
            else:
                logger.error(f"Unsupported export format: {self.ext}. Only .csv and .xlsx are supported.")
                raise ValueError(f"Unsupported export format: {self.ext}")
            logger.info(f"DataFrame successfully exported to {self.export_path}")
        except PermissionError:
            logger.error(f"Permission denied: {self.export_path}")
            raise
        except Exception as e:
            logger.error(f"Error occurred while exporting DataFrame: {e}")
            raise


if __name__ == "__main__":
    from config import config
    from src.loader_to_df import Loader
    logger.info("Test script started")
    # Sample dataframe loading
    sample_data = {
        "ID": [1, 2, 3],
        "Name": ["Alice", "Bob", "Charlie"],
        "Age": [25, 30, 35]
    }

    df = pd.DataFrame(sample_data)
    
    exporter = Exporter(df, Path("output_test.xlsx"))
    exporter.run_export()