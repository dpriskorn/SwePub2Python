import logging
from time import sleep
from typing import Dict, Set

import pandas as pd
from cache_to_disk import cache_to_disk
from wikibaseintegrator import wbi_config
from wikibaseintegrator.wbi_exceptions import MWApiError, SearchError
from wikibaseintegrator.wbi_helpers import mediawiki_api_call_helper

import config
from helpers.caching import read_from_cache, add_to_cache
from helpers.util import yes_no_question
from models.swedish_higher_education_authority import UKACodeLevel
from models.swepub.language import SwepubLanguage

wbi_config.config['USER_AGENT'] = config.user_agent
logger = logging.getLogger(__name__)


class SwepubSubject:
    """This models a Swepub subject aka topic"""
    data: Dict[str, str] = None
    label: str = None
    unnested_non_uka_labels: Set[str] = None
    language_code: SwepubLanguage = None
    # TODO decide whether to flesh out in own models UKACode or not
    uka_code: int = None
    uka_code_level: UKACodeLevel = None
    uka_label: str = None
    uka_scheme: bool = False
    matched_wikidata_qid: str = None
    manually_matched: bool = False

    def __init__(self,
                 data: Dict[str, str] = None,
                 label: str = None,
                 language_code: SwepubLanguage = None):
        if label is not None and language_code is not None:
            self.label = label
            self.language_code = language_code
        elif label is not None and language_code is None:
            logger.warning("Language code was missing on this subject. Setting to UNDETERMINED")
            self.label = label
            self.language_code = SwepubLanguage("und")
        else:
            if data is None:
                raise ValueError("data was None")
            else:
                # We got data, handle it
                self.data = data
                self.__parse_json__(data=data)
        if config.lookup_topics_in_wd and self.label is not None:
            self.__lookup_topic_in_wikidata__()

    @cache_to_disk(50)
    def __search_entities__(self, topic: str = None):
        """Looks up the code in WD using unfinished WBI function"""
        logger.info(f"Running sparql query for topic {topic}")
        # TODO convert 3-letter ISO code to Wikimedia language code using SPARQL
        # Code borrowed from https://github.com/LeMyst/WikibaseIntegrator/blob/32698a69d10d1cf50b18961d3fabc36111128883/wikibaseintegrator/wbi_helpers.py#L319
        # See license in that repo.
        # search_expression = (
        #         "-haswbstatement:P31=Q482994 "  # album
        #         "-haswbstatement:P31=Q13442814 "  # scientific article
        #         "-haswbstatement:P31=Q5633421 "  # scientific journal
        #         + topic
        # )
        # cirrus_search_params = {
        #     'action': 'query',
        #     'srsearch': search_expression,
        #     # hardcoded to English for now
        #     'list': "search",
        #     'srlimit': 50,
        #     'format': 'json'
        # }
        params = {
            'action': 'wbsearchentities',
            'search': topic,
            'site': "enwiki",
            # hardcoded to English for now
            'language': "en",
            'strict_language': False,
            'type': "item",
            'limit': 1,
            'format': 'json'
        }
        logger.info("Running search entities query on the Wikidata API")
        try:
            search_results = mediawiki_api_call_helper(data=params, allow_anonymous=True)
        except MWApiError:
            logger.error(f"Got {MWApiError} for {topic}")
            search_results = None
        if search_results is not None:
            if search_results['success'] != 1:
                raise SearchError('Wikibase API wbsearchentities failed')
            dict_result: bool = True
            results = []
            for i in search_results['search']:
                if dict_result:
                    description = i['description'] if 'description' in i else None
                    aliases = i['aliases'] if 'aliases' in i else None
                    results.append({
                        'id': i['id'],
                        'label': i['label'],
                        'match': i['match'],
                        'description': description,
                        'aliases': aliases
                    })
                else:
                    results.append(i['id'])
            return results

    def __ask_for_approval__(self, qid: str = None):
        """Lookup in cache and if miss ask for approval"""
        result = read_from_cache(label=self.label, qid=qid)
        logger.info(f"Result from cache was {result}")
        if result is None:
            if yes_no_question("Accept match?"):
                # TODO save match locally so that the user
                #  does not have to accept the same match twice
                self.matched_wikidata_qid = qid
                self.manually_matched = True
                logger.info("Match accepted")
                result = True
            else:
                logger.info("Match rejected")
                result = False
            add_to_cache(label=self.label, qid=qid, result=result)
        else:
            logger.info(f"Got cached result {result}")
            if result:
                self.matched_wikidata_qid = qid
                self.manually_matched = True

    def __lookup_topic_in_wikidata__(self):
        """Lookup this topic in Wikidata using WBI"""
        # We don't match UKÄ labels because we have a property proposal
        # underway and manual import/matching
        if not self.uka_scheme:
            results = self.__search_entities__(topic=self.label)
            logger.info(self.__search_entities__.cache_info())
            # pprint(results)
            # if only one result and the labels match exact save it automatically
            results_length = len(results)
            if results_length == 1:
                qid = results[0]["id"]
                label = results[0]["label"]
                description = results[0]["description"]
                print(f"We found exactly one result for '{self.label}': "
                      f"'{label}' with the description: '{description}' (see QID {qid})")
                self.__ask_for_approval__(qid=qid)
            elif results_length > 1:
                # TODO present choice to user and give them a
                #  short timespan of 5-10 seconds before auto-picking the first?
                # pick the first one for now because it is almost always right anyway
                # pprint(results)
                qid = results[0]["id"]
                label = results[0]["label"]
                description = results[0]["description"]
                print(f"Got {results_length} matches for '{self.label}' and "
                      f"auto-picked the first one '{label}' with the "
                      f"description: '{description}' (see QID {qid})")
                self.__ask_for_approval__(qid=qid)
            else:
                logger.warning(f"Got zero results from Wikidata for '{self.label}'")
            sleep(config.sleep_after_topic_match)
            # print("debug exit")
            # exit(0)
            # if multiple matches, present the results and move on

    def __parse_json__(self, data: Dict[str, str] = None):
        if "inScheme" in data:
            scheme_code = None
            scheme = data["inScheme"]
            logging.debug(f"Parsing scheme: {scheme}")
            if "code" in scheme:
                scheme_code = scheme["code"]
                # pprint(scheme_code)
                if scheme_code == "uka.se":
                    self.uka_scheme = True
                    logging.info(f"Found UKÄ 2016 scheme")
                else:
                    self.uka_scheme = False
                    logger.warning(f"Did not recognize the scheme {scheme_code}")
        else:
            logger.info("No scheme in this subject")
            scheme_code = None
        if "code" in data:
            code = data["code"]
            if self.uka_scheme:
                self.uka_code = int(code)
                if len(code) == 5:
                    logger.debug("Detected UKACodeLevel.FIVE")
                    self.uka_code_level = UKACodeLevel.FIVE
                elif len(code) == 3:
                    logger.debug("Detected UKACodeLevel.THREE")
                    self.uka_code_level = UKACodeLevel.THREE
                elif len(code) == 1:
                    logger.debug("Detected UKACodeLevel.ONE")
                    self.uka_code_level = UKACodeLevel.ONE
                else:
                    logger.warning("Unrecognized length of UKÄ code: {code}")
            else:
                logger.warning(f"Code {code} in unsupported scheme {scheme_code} detected")
        if "prefLabel" in data:
            code_label = data["prefLabel"]
            if self.uka_scheme:
                self.uka_label = code_label
            else:
                self.unnested_non_uka_labels = set()
                if "; " in code_label:
                    for label in code_label.split("; "):
                        self.unnested_non_uka_labels.add(label)
                else:
                    # Single label that we can set directly
                    self.label = code_label
        if "language" in data:
            language = data["language"]
            if "code" in data["language"]:
                self.language_code = SwepubLanguage(code=language["code"])
            else:
                logger.warning(
                    f"No language code found for the language {language} of this subject")
        if self.uka_scheme and self.uka_label and self.uka_code:
            logging.info(f"Found UKÄ 2016 code {self.uka_code} with label "
                         f"{self.uka_label} in the language {self.language_code}")
        else:
            if self.uka_code:
                logging.warning(
                    f'Found UKÄ code but no label: {self.uka_code}')

    def __str__(self):
        if self.uka_scheme:
            logging.info("__str__:Detected UKÄ scheme")
            if self.uka_code_level is None:
                raise ValueError(f"code_level was none for {self.data}")
            prefix = "UKÄ"
            if self.language_code is None:
                language = "no language code found"
            else:
                language = self.language_code.label
            return (f"{prefix}: "
                    f"{self.uka_label} ({language})\n"
                    f"UKÄ level {self.uka_code_level.value}: {self.uka_code}\n")
        else:
            logging.info("__str__:Detected Non-UKÄ scheme")
            prefix = "Non-UKÄ"
            if self.label is not None:
                # TODO lookup in WD with WBI
                if self.language_code is None:
                    language = "no language code found"
                else:
                    language = self.language_code.label
                return (f"{prefix}: "
                        f"{self.label} ({language})")
            else:
                raise ValueError(f"Could not print this subject {dict(self.data)}")

    def export_dataframe(self):
        # We don't export the labels attribute because it is nested.
        data = dict(
            language_code=self.language_code.code,
            language_label=self.language_code.label,
            language_qid=self.language_code.wikidata_qid,
            label=self.label,
            uka_code=self.uka_code,
            uka_code_level=self.uka_code_level,
            uka_label=self.uka_label,
            uka_scheme=self.uka_scheme,
            matched_wikidata_qid=self.matched_wikidata_qid,
            manually_matched=self.manually_matched,
        )
        # The list around data is needed because we have scalar values
        return pd.DataFrame(data=[data])