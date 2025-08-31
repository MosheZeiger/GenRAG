import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)



def merge_and_compare(
        df1: pd.DataFrame,
        left_on: list,
        df2: pd.DataFrame,
        right_on: list
) -> pd.DataFrame:
    """
    Merges two DataFrames on the specified columns and compares their metadata.
    Adds a 'status' column to indicate if the metadata matches or not.

    Parameters:
    df1 (pd.DataFrame): First DataFrame containing file metadata.
    df2 (pd.DataFrame): Second DataFrame containing file metadata.

    Returns:
    pd.DataFrame: Merged DataFrame with comparison results.
    """
    # Merge the two DataFrames on the specified columns
    try:
        if len(left_on) != len(right_on):
            error_message = "The number of columns to join on must be the same."
            logger.error(error_message)
            raise ValueError(error_message)
        if not left_on or not right_on:
            error_message = "At least one column must be specified for joining."
            logger.error(error_message)
            raise ValueError(error_message)
        print(f"""
              *******************************
              *******************************
              *******************************

              
              The df1 from left includes the following columns:
                {df1.columns.tolist()}
                one or more columns from the left on are missing. the columns left on are:
                {left_on}

                
              *******************************


                the columns left on are: {left_on}


              *******************************
              *******************************
              *******************************


              The df2 from right includes the following columns:

                {df2.columns.tolist()}

            
              *******************************


                the columns left on are: {right_on}
              """)
        if not all(col in df1.columns for col in left_on):
            error_message = f"""
            The df1 from left includes the following columns:
            {df1.columns.tolist()}
            one or more columns from the left on are missing. the columns left on are:
            {left_on}
            """
            logger.error(error_message)
            raise KeyError(error_message)
        if not all(col in df2.columns for col in right_on):
            error_message = f"""
            The df2 from right includes the following columns:
            {df2.columns.tolist()}
            one or more columns from the right on are missing. the columns right on are:
            {right_on}
            """
            logger.error(error_message)
            raise KeyError(error_message)
        logger.info(f"Merging DataFrames left on {left_on} right on {right_on}")

        merged_df = pd.merge(
            df1,
            df2,
            left_on=left_on,
            right_on=right_on,
            suffixes=('_df1', '_df2'),
            how='outer',
            indicator=True
        )

        merged_df.rename(columns={'_merge': 'source'}, inplace=True)
        source_mapping = {
            'left_only': 'Only in df1',
            'right_only': 'Only in df2',
            'both': 'In both'
        }
        merged_df['source'] = merged_df['source'].map(source_mapping)

        logger.info(f"Merged DataFrame shape: {merged_df.shape}")
    except Exception as e:
        logger.critical(f"Failed to merge DataFrames: {e}", exc_info=True)
        raise

    return merged_df

if __name__ == "__main__":
    from config import config
    from src.loader_to_df import Loader
    from my_common_utils.logger_setup import Logger

    Logger(console_level=logging.INFO)

    df1_left_path = config.DF1_SRC_PATH
    df2_right_path = config.DF2_SRC_PATH

    loader1 = Loader(df1_left_path)
    df1_left_data = loader1.get_dataframe()

    loader2 = Loader(df2_right_path)
    df2_right_data = loader2.get_dataframe()

    df1_left_on = config.DF1_ON_COLUMNS
    df2_right_on = config.DF2_ON_COLUMNS

    try:
        merged_data = merge_and_compare(
            df1=df1_left_data,
            df2=df2_right_data,
            left_on=df1_left_on,
            right_on=df2_right_on
        )
        print("Merge successful. Result head:")
        print(merged_data.head())
        print("\nSource column values:")
        print(merged_data['source'].value_counts())
    except (ValueError, KeyError) as e:
        logger.error(f"Failed to merge data due to a configuration error: {e}")
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)
