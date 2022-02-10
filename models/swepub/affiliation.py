import logging
from pprint import pprint
from typing import Dict, Any, List, Optional

import pandas as pd  # type: ignore
from pydantic import BaseModel
from wikibaseintegrator import wbi_config  # type: ignore

import config
from models.swepub.language import SwepubLanguage

wbi_config.config['USER_AGENT'] = config.user_agent
logger = logging.getLogger(__name__)


class SwepubAffiliation:
    """This models the affiliation aka organization and departments of authors in the Swepub raw_data"""
    name: Optional[str] = None
    language_code: Optional[SwepubLanguage] = None
    local_identifier: str = None
    has_subaffiliation: bool = False
    subaffiliations: Optional[List[Dict[str, Any]]] = None
    linked_to_person: bool
    url: Optional[str] = None

    def __init__(
            self,
            affiliation: Dict[str, Any] = None,
            linked_to_person: bool = True
    ):
        self.linked_to_person = linked_to_person
        if isinstance(affiliation, list):
            raise ValueError("got list, need Dict")
        self.__parse__(affiliation)

    def __parse__(self, affiliation):
        if affiliation is None:
            raise ValueError("affiliation was None")
        #pprint(affiliation)
        if "@type" in affiliation:
            affiliation_type: str = affiliation["@type"]
            if affiliation_type.strip() == "Organization":
                logger.debug("parsing organization:")
                if "name" in affiliation:
                    name = affiliation["name"]
                    if name != "":
                        self.name = name
                    else:
                        logger.warning("name of organization was empty string")
                else:
                    logger.warning(f"no name for the affiliation {affiliation}")
                if "language" in affiliation:
                    language = affiliation["language"]
                    if "code" in language:
                        self.language_code = SwepubLanguage(code=language["code"])
                    else:
                        self.language_code = SwepubLanguage(code="und")
                        logger.warning(
                            f"No language code found for the language {language} "
                            f"of this organization name")
                else:
                    logger.info(f"language was missing for this organization {self.name}, setting to 'und'")
                    self.language_code = SwepubLanguage(code="und")
                if "identifiedBy" in affiliation:
                    identifiers = affiliation["identifiedBy"]
                    for identifier in identifiers:
                        if "@type" in identifier:
                            identifier_type = identifier["@type"]
                            value = identifier["value"]
                            if identifier_type == "Local":
                                self.local_identifier = value
                            elif identifier_type == "URI":
                                    self.url = value
                            else:
                                logger.warning(f"unsupported identifier {identifier_type} in swepub affiliation")
            else:
                logger.warning(f"unsupported affiliation type {affiliation_type} found")
        if "hasAffiliation" in affiliation:
            self.has_subaffiliation = True
            self.subaffiliations = []
            subaffiliations: List[Dict[str, Any]] = affiliation["hasAffiliation"]
            for subaffiliation in subaffiliations:
                self.subaffiliations.append(subaffiliation)
            logger.info(f"{len(self.subaffiliations)} subaffiliation(s) found for {self.name}")
        if self.name is None:
            logger.warning("no name found on this affiliation")
            #print(self)
            #exit()

    def __str__(self):
        if self.language_code is None:
            self.language_code = SwepubLanguage("und")
        return f"{self.name} ({self.language_code.label})"

    def export_dataframe(self):
        if self.language_code is None:
            self.language_code = SwepubLanguage("und")
        data = dict(
            name=self.name,
            local_identifier=self.local_identifier,
            had_nested_affiliations=self.has_subaffiliation,
            language_code=self.language_code.code,
            linked_to_person=self.linked_to_person,
            url=self.url
        )
        # The list around raw_data is needed because we have scalar values
        return pd.DataFrame(data=[data])
