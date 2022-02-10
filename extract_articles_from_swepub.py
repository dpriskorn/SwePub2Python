import logging
import os
import time
import zipfile
from pathlib import Path

import pandas as pd

from helpers.dataframe import save_dataframe_to_pickle
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
