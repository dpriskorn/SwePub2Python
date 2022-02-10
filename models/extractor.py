import logging
import os
import time
import zipfile

import pandas as pd
from pydantic import BaseModel

import config
from models.swepub.article import SwepubArticle

# This script is intended to be run on the WMC Kubernetes cluster

logger = logging.getLogger(__name__)


class Extractor(BaseModel):
    """
    This class extracts from SwePub unspecified JSON into Python objects
    and stores them in a dataframe and pickles it to disk

    The source file is huge. 1.3GB gzipped JSONL
    There are ~1.4M lines in total

    Defaults to article_pickle_filename = "swepub.pkl.gz"
    """
    swepub_deduplicated_zipfile_path: str = None
    article_pickle_filename: str = "articles.pkl.gz"
    subjects_pickle_filename: str = "subjects.pkl.gz"
    stop_line_number: int = 100
    begin_line_number: int = 1
    show_progress_every_x_line: int = 10

    def extract(self):
        if self.swepub_deduplicated_zipfile_path is None:
            raise ValueError("swepub_deduplicated_zipfile_path was None")
        if self.begin_line_number > self.stop_line_number:
            raise ValueError("cannot begin higher than the stop line number")
        logger.info("Beginning extraction")
        start = time.time()
        articles_df = pd.DataFrame()
        subjects_df = pd.DataFrame()
        # This probably read the whole thing into memory which is not ideal...
        with zipfile.ZipFile(self.swepub_deduplicated_zipfile_path) as z:
            for filename in z.namelist():
                if not os.path.isdir(filename):
                    # read the file
                    with z.open(filename) as f:
                        current_line_number = 1
                        for line in f:
                            # print(line)
                            if current_line_number % self.show_progress_every_x_line == 0:
                                progress = round((current_line_number - self.begin_line_number) * 100 /
                                                 (self.stop_line_number - self.begin_line_number))
                                print(f"count:{current_line_number} duration:{round(time.time() - start)}s "
                                      f"start:{self.begin_line_number} stop:{self.stop_line_number} "
                                      f"progress{progress}%",
                                      flush=True)
                            if current_line_number >= self.begin_line_number:
                                article = SwepubArticle(raw_data=line)
                                df_article = article.export_dataframe()
                                if article.subjects is not None:
                                    df_subject = pd.DataFrame()
                                    for subject in article.subjects:
                                        df_subject = df_subject.append(subject.export_dataframe())
                                    # add article id as index to all rows
                                    # https://www.geeksforgeeks.org/add-column-with-constant-value-to-pandas-dataframe/
                                    df_subject['id'] = pd.Series([article.id for x in range(len(df_subject.index))])
                                    if config.loglevel == logging.DEBUG:
                                        print(df_subject.describe())
                                        exit()
                                    subjects_df = subjects_df.append(df_subject)
                                articles_df = articles_df.append(df_article)
                            elif current_line_number == self.stop_line_number:
                                logger.warning("Reached stop line number")
                                break
                            else:
                                break
                            current_line_number += 1
        print(f"starting to save article pickle {self.article_pickle_filename} now", flush=True)
        articles_df.to_pickle(self.article_pickle_filename, protocol=5)
        print(f"saved to pickle {self.article_pickle_filename}", flush=True)
        print(f"starting to save subjects pickle {self.article_pickle_filename} now", flush=True)
        subjects_df.to_pickle(self.subjects_pickle_filename, protocol=5)
        end = time.time()
        print(f"total duration: {round(end - start)}s")
