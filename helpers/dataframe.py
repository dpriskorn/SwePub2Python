from os.path import exists
from typing import Union

from pandas import DataFrame

from extract_articles_from_swepub import logger, filename_prefix, begin_line_number


def save_dataframe_to_pickle(previous_save_at_line_number: int = None,
                             current_line_number: Union[int, str] = None,
                             df: DataFrame = None):
    if current_line_number is None:
        raise ValueError("count was None")
    if df is None:
        raise ValueError("df was None")
    # We use zfill to make sure they sort nice in the filesystem
    if previous_save_at_line_number is None:
        logger.debug("saving using begin_line_number")
        pickle_filename = f"{filename_prefix}.{str(begin_line_number).zfill(6)}-{str(current_line_number).zfill(6)}.pkl.gz"
    else:
        logger.debug("saving using previous_saved_count")
        pickle_filename = f"{filename_prefix}.{str(previous_save_at_line_number).zfill(6)}-{str(current_line_number).zfill(6)}.pkl.gz"
    # We use the highest protocol which works in Python 3.9
    # but perhaps not in earlier versions
    if exists(pickle_filename):
        print(f"{pickle_filename} already exists, overwriting", flush=True)
    print(f"starting to save pickle {pickle_filename} now", flush=True)
    df.to_pickle(pickle_filename, protocol=5)
    print(f"saved to pickle {pickle_filename}", flush=True)