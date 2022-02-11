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
    affiliations_pickle_filename: str = "affiliations.pkl.gz"
    contributors_pickle_filename: str = "contributors.pkl.gz"
    subjects_pickle_filename: str = "subjects.pkl.gz"
    stop_line_number: int = config.stop_line_number
    start_line_number: int = config.start_line_number
    show_progress_every_x_line: int = 10

    def extract(self):
        if self.swepub_deduplicated_zipfile_path is None:
            raise ValueError("swepub_deduplicated_zipfile_path was None")
        if self.start_line_number > self.stop_line_number:
            raise ValueError("cannot begin higher than the stop line number")
        logger.info("Beginning extraction")
        start = time.time()
        articles_df = pd.DataFrame()
        subjects_df = pd.DataFrame()
        contributors_df = pd.DataFrame()
        affiliations_df = pd.DataFrame()
        # This probably read the whole thing into memory which is not ideal...
        with zipfile.ZipFile(self.swepub_deduplicated_zipfile_path) as z:
            for filename in z.namelist():
                if not os.path.isdir(filename):
                    # read the file
                    with z.open(filename) as f:
                        current_line_number = 1
                        for line in f:
                            # print(current_line_number)
                            # print(line)
                            if current_line_number % self.show_progress_every_x_line == 0:
                                progress = round((current_line_number - self.start_line_number) * 100 /
                                                 (self.stop_line_number - self.start_line_number))
                                print(f"count:{current_line_number} duration:{round(time.time() - start)}s "
                                      f"start:{self.start_line_number} stop:{self.stop_line_number} "
                                      f"progress{progress}%",
                                      flush=True)
                            if current_line_number >= self.start_line_number:
                                article: SwepubArticle = SwepubArticle(raw_data=line)
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
                                if article.contributors is not None:
                                    df_contributors = pd.DataFrame()
                                    for contributor in article.contributors:
                                        df_contributors = df_contributors.append(contributor.export_dataframe())
                                        if contributor.affiliations is not None:
                                            df_affiliations = pd.DataFrame()
                                            for affiliation in contributor.affiliations:
                                                df_affiliations = df_affiliations.append(affiliation.export_dataframe())
                                            # add article id as index to all rows
                                            # https://www.geeksforgeeks.org/add-column-with-constant-value-to-pandas-dataframe/
                                            df_affiliations['id'] = pd.Series(
                                                [article.id for x in range(len(df_affiliations.index))]
                                            )
                                            if config.loglevel == logging.DEBUG:
                                                print(df_affiliations.describe())
                                                exit()
                                            affiliations_df = affiliations_df.append(df_affiliations)
                                    # add article id as index to all rows
                                    # https://www.geeksforgeeks.org/add-column-with-constant-value-to-pandas-dataframe/
                                    df_contributors['id'] = pd.Series(
                                        [article.id for x in range(len(df_contributors.index))])
                                    if config.loglevel == logging.DEBUG:
                                        print(df_contributors.describe())
                                        exit()
                                    contributors_df = contributors_df.append(df_contributors)
                                articles_df = articles_df.append(df_article)
                            if current_line_number == self.stop_line_number:
                                logger.warning("Reached stop line number")
                                break
                            current_line_number += 1
        print(f"starting to save article pickle {self.article_pickle_filename} now", flush=True)
        articles_df.to_pickle(self.article_pickle_filename, protocol=5)
        print(f"saved to pickle {self.article_pickle_filename}", flush=True)
        print(f"starting to save subjects pickle {self.article_pickle_filename} now", flush=True)
        subjects_df.to_pickle(self.subjects_pickle_filename, protocol=5)
        print(f"saved to pickle {self.subjects_pickle_filename}", flush=True)
        contributors_df.to_pickle(self.contributors_pickle_filename)
        affiliations_df.to_pickle(self.affiliations_pickle_filename)
        end = time.time()
        print(f"total duration: {round(end - start)}s")
