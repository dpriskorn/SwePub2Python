import logging
from os.path import exists

import pandas as pd

logger = logging.getLogger(__name__)

# lookups where inspired by
# https://stackoverflow.com/questions/24761133/pandas-check-if-row-exists-with-certain-values


def read_from_cache(
        label: str = None,
        qid: str = None,
):
    """Returns None or result from the cache"""
    if label is None or qid is None:
        raise ValueError("did not get all we need")
    logger.info("Reading from the cache")
    if exists("cache.pkl"):
        df = pd.read_pickle("cache.pkl")
        # This tests whether any row matches
        match = ((df['qid'] == qid) & (df['label'] == label)).any()
        logger.debug(f"match:{match}")
        if match:
            # Here we find the row that matches and extract the
            # result column and extract the value using any()
            result = df.loc[df["qid"] == qid, "result"].any()
            logger.debug(f"result:{result}")
            if result is not None:
                return result


def add_to_cache(
        label: str = None,
        qid: str = None,
        result: bool = None
):
    if label is None or qid is None or result is None:
        raise ValueError("did not get all we need")
    logger.info("Adding to cache")
    data = dict(label=label, qid=qid, result=result)
    if exists("cache.pkl"):
        df = pd.read_pickle("cache.pkl")
        # This tests whether any row matches
        match = ((df['qid'] == qid) & (df['label'] == label)).any()
        logger.debug(f"match:{match}")
        if not match:
            # We only give save the value once for now
            df = df.append(pd.DataFrame(data=[data]))
            df.to_pickle("cache.pkl")
    else:
        pd.DataFrame(data=[data]).to_pickle("cache.pkl")
