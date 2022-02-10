import json
import logging
from json import JSONDecodeError
from typing import List, Optional, Dict, Any

import pandas as pd  # type: ignore
from langdetect import detect, LangDetectException
from pydantic import BaseModel
from wikibaseintegrator import wbi_config  # type: ignore

import config
from models.swedish_higher_education_authority import UKACodeLevel
from models.swepub.contributor import SwepubContributor
from models.swepub.language import SwepubLanguage
from models.swepub.subject import SwepubSubject

wbi_config.config['USER_AGENT'] = config.user_agent
logger = logging.getLogger(__name__)


class SwepubArticle:
    """This class parses a Swepub article json into an object"""
    abstracts: Optional[List[str]] = None
    contributors: Optional[List[SwepubContributor]] = None
    detected_abstract_language: Optional[str] = None
    doi: Optional[str] = None
    hdl: Optional[str] = None  # What is this?
    id: Optional[str] = None
    isbn: Optional[str] = None
    isi: Optional[str] = None  # What is this?
    issn: Optional[str] = None
    # json: Optional[Dict[str, str]] = None
    libris_id: Optional[str] = None
    language_codes: Optional[List[SwepubLanguage]] = None
    unknown_local_identifier: Optional[str] = None  # What is this?
    patent_number: Optional[str] = None
    pmid: Optional[str] = None
    scopusid: Optional[str] = None
    titles: Optional[List[str]] = None
    subjects: Optional[List[SwepubSubject]] = None
    url: Optional[str] = None
    number_of_abstracts: int = 0
    number_of_titles: int = 0
    number_of_language_codes: int = 0
    number_of_contributors: int = 0
    raw_data: Any = None

    def parse(self):
        try:
            deserialized_json_data = json.loads(self.raw_data)
            self.__parse_json__(data=deserialized_json_data)
        except JSONDecodeError:
            logger.error(f"Decoding of the json {self.raw_data} failed")

    def __parse_json__(self, data: Any):
        def __parse_abstracts__(instance_of: Any):
            if config.parse_abstracts:
                if "summary" in instance_of:
                    summaries: List[Dict[str, str]] = instance_of["summary"]
                    logger.info(f"Found {len(summaries)} abstracts")
                    if len(summaries) > 0:
                        self.abstracts = []
                        # TODO create a models for abstracts with language_code and text attributes
                        # Detect the language of the abstract
                        if config.detect_language_of_abstract:
                            if "label" in summaries[0]:
                                first_summary = summaries[0]["label"]
                                if len(first_summary.strip()) > 0:
                                    logger.info(f"First abstract found: {first_summary}")
                                    try:
                                        self.detected_abstract_language = detect(first_summary)
                                        logger.info(f"Detected language of the "
                                                    f"first abstract : {self.detected_abstract_language}")
                                    except LangDetectException:
                                        logger.error(f"Could not detect language"
                                                     f"for summary: {first_summary}")
                                else:
                                    logger.warning("First summary was empty.")
                        for summary in summaries:
                            if "label" in summary:
                                self.abstracts.append(summary["label"])
                        self.number_of_abstracts = len(self.abstracts)
                    else:
                        logger.info("No summary found for this article")
                else:
                    logger.info("No summery found for this article")

        def __parse_contributors__(instance_of: Any):
            if config.parse_contributors:
                if "contribution" in instance_of:
                    # This can hold bot authors and other stakeholders
                    self.contributors = []
                    contributions = instance_of["contribution"]
                    for person in contributions:
                        self.contributors.append(SwepubContributor(person))
                    self.number_of_contributors = len(self.contributors)

        def __parse_identifiers__(master: Any):
            if "@id" in master:
                self.id: str = master["@id"]
            else:
                raise ValueError("id was None")
            # We detect the identifiers first so we can link to them in some messages below
            if "identifiedBy" in master:
                logger.info("Found identifiedBy")
                identified_by = master["identifiedBy"]
                logger.debug(f"identified_by:{identified_by}")
                for item in identified_by:
                    if "@type" in item:
                        identifier_type = item["@type"]
                        value = item["value"]
                        if identifier_type == "URI":
                            self.url = value
                        elif identifier_type == "DOI":
                            self.doi = value
                        elif identifier_type == "PMID":
                            self.pmid = value
                        elif identifier_type == "ScopusID":
                            self.scopusid = value
                        elif identifier_type == "ISBN":
                            self.isbn = value
                        elif identifier_type == "Local":
                            self.unknown_local_identifier = value
                        elif identifier_type == "LibrisNumber":
                            self.libris_id = value
                        elif identifier_type == "PatentNumber":
                            self.patent_number = value
                        elif identifier_type == "ISI":
                            self.isi = value
                        elif identifier_type == "Hdl":
                            self.hdl = value
                        elif identifier_type == "ISSN":
                            self.issn = value
                        else:
                            logger.warning(f"Unsupported identifier_type {identifier_type} "
                                           f"detected with value {value}")
            else:
                logger.info(f"No identifiedBy found under master for this article")

        def __parse_language__(instance_of: Any):
            if "language" in instance_of:
                self.language_codes = []
                languages: List[Dict[str, str]] = instance_of["language"]
                for language in languages:
                    if "code" in language:
                        self.language_codes.append(SwepubLanguage(language["code"]))
                self.number_of_language_codes = len(self.language_codes)

        def __parse_tiles__(instance_of: Any):
            if config.parse_titles:
                if "hasTitle" in instance_of:
                    has_title: List[Dict[str, str]] = instance_of["hasTitle"]
                    # print(type(has_title))
                    logger.info(f"Found {len(has_title)} titles")
                    title_labels = []
                    self.titles = []
                    for title in has_title:
                        if "mainTitle" in title:
                            title_label = title["mainTitle"]
                            # Check for empty string
                            if title_label.strip() != "":
                                self.titles.append(title_label)
                                if len(self.language_codes) == 1:
                                    logging.info(
                                        f"Title {title_label} found in work with "
                                        f"the language {self.language_codes[0]}")
                                elif len(self.language_codes) > 1:
                                    logging.info(
                                        f"Title {title_label} found in work with one of "
                                        f"the languages {self.language_codes}")
                                else:
                                    logging.info(f"Title {title_label} found"
                                                 f"in work with unknown language")
                        else:
                            logger.warning(f"No main title found for this article at {self.url}")
                    self.number_of_titles = len(self.titles)

        def __parse_subjects__(instance_of: Any):
            if config.parse_subjects:
                if "subject" in instance_of:
                    # This holds all the subjects
                    json_subjects: List[Dict[str, str]] = instance_of["subject"]
                    logger.info(f"Found {len(json_subjects)} subjects")
                    # logger.debug("subjects:")
                    # pprint(subjects)
                    if len(json_subjects) > 0:
                        self.subjects = []
                        for subject_json_item in json_subjects:
                            logger.debug(f"subject type:{type(subject_json_item)}")
                            logger.debug(f"subject data:")
                            # pprint(subject_json_item)
                            subject = SwepubSubject(data=subject_json_item)
                            if subject.unnested_non_uka_labels is not None:
                                logger.info(f"Unnesting {len(subject.unnested_non_uka_labels)} labels")
                                for label in subject.unnested_non_uka_labels:
                                    logger.debug(f"unnesting label:{label}")
                                    unnested_subject = SwepubSubject(
                                        label=label,
                                        # Inherit the language code
                                        language_code=subject.language_code
                                    )
                                    self.subjects.append(unnested_subject)
                            else:
                                self.subjects.append(subject)

        master: Any = data["master"]
        __parse_identifiers__(master)
        # pprint(master)
        if "instanceOf" in master:
            instance_of: Dict[str, List[Dict[str, str]]] = master["instanceOf"]
            __parse_abstracts__(instance_of)
            __parse_contributors__(instance_of)
            __parse_language__(instance_of)
            __parse_tiles__(instance_of)
            __parse_subjects__(instance_of)

    def __str__(self):
        """Prints an article object in a way that makes it easy for the user to read"""
        if config.parse_contributors and config.parse_titles and config.parse_abstracts:
            return (
                f"id:{self.id}\n"
                f"titles:{self.titles}\n"
                f"url:{self.url}\n"
                f"doi:{self.doi}\n"
                f"pmid:{self.pmid}\n"
                f"scopusid:{self.scopusid}\n"
                f"non-swedish UKÄ level five subjects: {[str(subject) for subject in self.non_swedish_uka_subjects_with_specific_code_level(level=UKACodeLevel.FIVE)]}\n"
                f"non-swedish UKÄ level three subjects: {[str(subject) for subject in self.non_swedish_uka_subjects_with_specific_code_level(level=UKACodeLevel.THREE)]}\n"
                f"non-swedish UKÄ level one subjects: {[str(subject) for subject in self.non_swedish_uka_subjects_with_specific_code_level(level=UKACodeLevel.ONE)]}\n"
                f"non-swedish non-UKÄ subjects: {[str(subject) for subject in self.non_swedish_subjects_non_uka_subjects()]}"
            )
        else:
            return (
                f"id:{self.id}\n"
                f"doi:{self.doi}\n"
                f"pmid:{self.pmid}\n"
                f"scopusid:{self.scopusid}\n"
                f"url:{self.url}"
            )

    def export_dataframe(self):
        # This is not an optimal way of storing the raw_data in pandas
        # https://stackoverflow.com/questions/26792852/multiple-values-in-single-column-of-a-pandas-dataframe
        # https://stackoverflow.com/questions/26483254/python-pandas-insert-list-into-a-cell#47548471
        if config.parse_contributors and config.parse_titles and config.parse_abstracts:
            logger.info("Returning a dataframe with all information we currently support")
            if self.number_of_abstracts > 0:
                first_abstract = self.abstracts[0]
            else:
                first_abstract = None
            if self.number_of_titles > 0:
                first_title = self.titles[0]
            else:
                first_title = None
            data = dict(
                contributors=self.contributors,
                doi=self.doi,
                first_abstract=first_abstract,
                first_title=first_title,
                id=self.id,  # scalar value
                isbn=self.isbn,
                isi=self.isi,
                issn=self.issn,
                language_codes=self.language_codes,
                number_of_abstracts=self.number_of_abstracts,
                number_of_contributors=self.number_of_contributors,
                number_of_language_codes=self.number_of_language_codes,
                number_of_titles=self.number_of_titles,
                object=self,
                patent_number=self.patent_number,
                pmid=self.pmid,
                scopusid=self.scopusid,
                subjects=self.subjects,  # list
                url=self.url,
            )
            # print(raw_data)
            # The list around raw_data is needed because we have scalar values
            return pd.DataFrame(data=[data])  # , index=[count])
        else:
            logger.info("Exporting a minimal dataframe with identifiers and language codes")
            data = dict(
                doi=self.doi,
                id=self.id,  # scalar value
                isbn=self.isbn,
                isi=self.isi,
                issn=self.issn,
                language_codes=self.language_codes,  # list
                number_of_language_codes=self.number_of_language_codes,
                object=self,
                patent_number=self.patent_number,
                pmid=self.pmid,
                scopusid=self.scopusid,
                url=self.url,
            )
            # print(raw_data)
            # The list around raw_data is needed because we have scalar values
            return pd.DataFrame(data=[data])

    def non_swedish_subjects(self):
        """This filters out all subjects with the language_code=swe"""
        if self.subjects is not None:
            return list(filter(lambda x: x.language_code != "swe", self.subjects))
        else:
            return list()

    def non_swedish_subjects_non_uka_subjects(self):
        """This filters out all subjects with the language_code=swe and any uka_code set"""
        if self.subjects is not None:
            return list(filter(lambda x: (x.language_code != "swe" and
                                          x.uka_code is None),
                               self.subjects))
        else:
            return list()

    def non_swedish_uka_subjects(self):
        """This returns all subjects that have a UKÄ classification code but
        leaves out the ones with labels in Swedish"""
        if self.subjects is not None:
            return list(
                filter(
                    lambda x: (x.uka_code is not None and
                               x.language_code != "swe"),
                    self.subjects
                )
            )
        else:
            return list()

    def non_swedish_uka_subjects_with_specific_code_level(
            self,
            level: UKACodeLevel = None
    ):
        """This returns all subjects that have a UKÄ classification code level
        specified by LEVEL but leaves out the ones with labels in Swedish"""
        if level is None:
            raise ValueError("level was None")
        if not isinstance(level, UKACodeLevel):
            raise ValueError("level was not a valid UKACodeLevel")
        if self.subjects is not None:
            return list(
                filter(
                    lambda x: (x.uka_code is not None and
                               x.language_code != "swe" and
                               x.uka_code_level == level),
                    self.subjects
                )
            )
        else:
            return list()

    def uka_subjects(self):
        """This filters out all subjects that does not have a UKÄ classification code"""
        if self.subjects is not None:
            return list(filter(lambda x: x.uka_code is not None, self.subjects))
        else:
            return list()

    def uka_subjects_with_specific_code_level(
            self,
            level: UKACodeLevel = None
    ):
        """This returns all subjects that have a UKÄ classification
        code level specified by LEVEL"""
        if level is None:
            raise ValueError("level was None")
        if not isinstance(level, UKACodeLevel):
            raise ValueError("level was not a valid UKACodeLevel")
        if self.subjects is not None:
            return list(
                filter(
                    lambda x: (x.uka_code is not None and
                               x.uka_code_level == level),
                    self.subjects
                )
            )
        else:
            return list()