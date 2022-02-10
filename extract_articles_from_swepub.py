import logging
import os
import time
import zipfile
from os.path import exists
from pathlib import Path
from typing import Union

import pandas as pd
from pandas import DataFrame

from models.swepub.article import SwepubArticle

# This script is intended to be run on the WMC Kubernetes cluster

logging.basicConfig(level=config.loglevel)
logger = logging.getLogger(__name__)

# This file is huge. 1.3GB gzipped json
name = f'{Path.home()}/WikidataMLSuggester/swepub-deduplicated.zip'
# There are ~1.4M lines in total
stop_line_number = 1500000
begin_line_number = 710000
export_every_x_line_number = 1000
show_progress_every_x_line = 100
filename_prefix = "articles_710k-end"

# Checks
if begin_line_number > stop_line_number:
    raise ValueError("cannot begin higher than the stop line number")

# There is a lot of bloat in their choice of specification.
# E.g. titles of all the UKÄ codes could have been left out
# and put into a Wikibase graph database instead and just linked
# That would have saved a lot of space and bandwidth.

# Suggestions for improvements of the data models:
# 1) Add language codes to titles just as you do for summaries.


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


start = time.time()
df = pd.DataFrame()
previous_save_at_line_number = None
reached_stop_before_end_of_file = False
# This probably read the whole thing into memory which is not ideal...
with zipfile.ZipFile(name) as z:
    for filename in z.namelist():
        if not os.path.isdir(filename):
            # read the file
            with z.open(filename) as f:
                current_line_number = 1
                for line in f:
                    # print(line)
                    if current_line_number >= begin_line_number:
                        article = SwepubArticle(data=line)
                        df_article = article.export_dataframe()
                        df = df.append(df_article)
                        if current_line_number % show_progress_every_x_line == 0:
                            progress = round((current_line_number - begin_line_number) * 100 /
                                             (stop_line_number - begin_line_number))
                            print(f"count:{current_line_number} duration:{round(time.time() - start)}s "
                                  f"start:{begin_line_number} stop:{stop_line_number} "
                                  f"progress{progress}%",
                                  flush=True)
                        if current_line_number % export_every_x_line_number == 0:
                            # Workaround of bug in Kubernetes
                            # We export every export_every_x_linenumber because k8s cannot save
                            # a bigger pickle to disk without getting killed
                            save_dataframe_to_pickle(current_line_number=current_line_number,
                                                     df=df,
                                                     previous_save_at_line_number=previous_save_at_line_number)
                            previous_save_at_line_number = current_line_number
                            # Empty the dataframe
                        if current_line_number == stop_line_number:
                            save_dataframe_to_pickle(current_line_number=current_line_number,
                                                     df=df,
                                                     previous_save_at_line_number=previous_save_at_line_number)
                            reached_stop_before_end_of_file = True
                            break
                    current_line_number += 1
if not reached_stop_before_end_of_file:
    save_dataframe_to_pickle(current_line_number="last_line",
                             df=df,
                             previous_save_at_line_number=previous_save_at_line_number)
end = time.time()
print(f"total duration: {round(end - start)}s")
