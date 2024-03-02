import logging

from cache_to_disk import cache_to_disk  # type: ignore
from wikibaseintegrator import WikibaseIntegrator, wbi_config  # type: ignore
from wikibaseintegrator.wbi_helpers import execute_sparql_query  # type: ignore

import config
from helpers.wdqs import extract_the_first_wikibase_value_from_a_wdqs_result_set

wbi_config.config["USER_AGENT"] = config.user_agent
logger = logging.getLogger(__name__)


class IsoThreeLetterLanguageCode:
    """Official ISO 639-2 support"""

    code: str = None
    # We use str instead of LanguageValue here to enable
    # caching to disk using a decorator
    label: str = None
    wikidata_qid: str = None

    def __init__(self, code: str):
        if code is None:
            raise ValueError("code was None")
        else:
            self.code = code
            if config.lookup_languages_in_wd:
                self.__lookup_label__()
            else:
                self.label = f"code: {self.code} has not been looked up"

    @cache_to_disk(50)
    def __run_wdqs_query__(self, code: str = None):
        """Looks up the code in WD using P219 and SPARQL"""
        logger.info(f"Running sparql query for code. {code}")
        return execute_sparql_query(
            f"""
            SELECT ?item 
            WHERE 
            {{
              ?item wdt:P219 "{code}".
            }}
            """
        )

    @cache_to_disk(50)
    def __lookup_label_using_wbi__(self, item: str = None):
        if item is None:
            raise ValueError("item was None")
        wbi = WikibaseIntegrator(login=None)
        # For now we only get the English label and toss out the rest
        label = wbi.item.get(item).labels.get("en").value
        return label

    def __lookup_label__(self):
        """Calls functions to look up the label using WD"""
        if self.code is None:
            raise ValueError("self.code was None")
        # First we find the QID of the language by using SPARQL
        item = None
        result = self.__run_wdqs_query__(code=self.code)
        logger.debug(self.__lookup_label_using_wbi__.cache_info())
        if result is not None:
            logger.debug(f"wbi result:{result}")
            self.wikidata_qid = extract_the_first_wikibase_value_from_a_wdqs_result_set(
                json=result, sparql_variable="item"
            )
        if self.wikidata_qid is not None:
            self.label = self.__lookup_label_using_wbi__(item=self.wikidata_qid)
            logger.debug(self.__lookup_label_using_wbi__.cache_info())
        else:
            # Hardcode workaround for issue #4
            if self.code == "ger":
                self.label = "German"
            elif self.code == "scr":
                self.label = "Undetermined"
            elif self.code == "scc":
                self.label = "Undetermined"
            else:
                logger.error(
                    f"no item found for ISO 639-2 code '{self.code}' in Wikidata, "
                    f"falling back to 'undetermined'"
                )
                self.label = "Undetermined"

    def __str__(self):
        return f"{self.label}"
