import logging

import config
from models.extractor import Extractor
import sys

logging.basicConfig(level=config.loglevel)


def extract_to_dataframes(zipfile_path):
    extractor = Extractor(swepub_deduplicated_zipfile_path=zipfile_path)
    extractor.extract()


def main():
    if len(sys.argv) != 2:
        print("Usage: python script_name.py <zipfile_path>")
        sys.exit(1)

    zipfile_path = sys.argv[1]
    extract_to_dataframes(zipfile_path)


if __name__ == "__main__":
    main()
