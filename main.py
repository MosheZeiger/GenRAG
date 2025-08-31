import logging
from pathlib import Path

from config import config
from my_common_utils.logger_setup import Logger

from src.files_metadata_exporter import runner as metadata_runner
from src.loader_to_df import Loader
from src.data_comparator import merge_dataframes



logger = logging.getLogger(__name__)

def main_pipeline():
    """
    Orchestrates the entire data processing pipeline.
    """
    logger.info("Starting main pipeline...")

    logger.info("Step 1: Running metadata extraction...")
    path_to_scan = config.PATH_TO_SCAN
    metadata_df = metadata_runner(root_folder_path=path_to_scan)
    logger.info(f"Metadata extraction completed. Extracted {len(metadata_df)} records.")

    logger.info("Step 2: Loading data to comparison...")
    comparison_file_path = config.DF2_SRC_PATH
    loader = Loader(source_file=comparison_file_path)
    comparison_df = loader.get_dataframe()
    logger.info(f"Loaded comparison data from {comparison_file_path}. Extracted {len(comparison_df)} records.")

    logger.info("Step 3: Merging data...")
    on_df1_columns = config.DF1_ON_COLUMNS
    on_df2_columns = config.DF2_ON_COLUMNS
    merged_df = merge_dataframes(
        df1=metadata_df,
        on_df1_columns=on_df1_columns,
        df2=comparison_df,
        on_df2_columns=on_df2_columns
    )
    logger.info(f"Merging completed. Merged DataFrame has {len(merged_df)} records.")
    print(merged_df.head(5))





if __name__ == "__main__":
    Logger(console_level=logging.INFO)
    try:
        main_pipeline()
    except Exception as e:
        logger.critical(f"pipeline failed with a critical error. Process halted", exc_info=True)