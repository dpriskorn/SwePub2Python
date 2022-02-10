import logging

# import click

import config
from models.extractor import Extractor
from models.swepub.dataframe import SwepubDataframe

logging.basicConfig(level=config.loglevel)

# @click.option('--extract', help='Extract SwePub zip to Python objects in a dataframe')
# @click.option('--export-subjects', help='Export the subjects in a dataframe')

# @click.command()
def extract_to_dataframes():
    extractor = Extractor(swepub_deduplicated_zipfile_path='swepub-deduplicated.zip')
    extractor.start()


# @click.command()
# def export_subjects():
#     sdf = SwepubDataframe()
#     sdf.load_into_memory()
#     sdf.export_subjects_dataframe()


def main():
    extract_to_dataframes()


if __name__ == '__main__':
    main()
