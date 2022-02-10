import logging
from typing import Any, List, Dict, Optional

import pandas as pd  # type: ignore
from pydantic import BaseModel
from wikibaseintegrator import wbi_config  # type: ignore

import config
from models.swepub.affiliation import SwepubAffiliation

wbi_config.config['USER_AGENT'] = config.user_agent
logger = logging.getLogger(__name__)


class SwepubContributor:
    """This models the contributor aka author in the Swepub raw_data"""
    given_name: Optional[str] = None
    family_name: Optional[str] = None
    affiliations: List[SwepubAffiliation]
    orcid: Optional[str] = None
    local_identifier: Optional[str] = None

    def __init__(self, person_data: Any = None):
        if person_data is None:
            raise ValueError("instance of was None")
        else:
            self.affiliations = []
            self.__parse__(person_data)

    def __parse__(self, contributor_data):
        if contributor_data is None:
            raise ValueError("raw_data was None")
        # pprint(person)
        if "agent" in contributor_data:
            # This designates the role of the agent
            agent = contributor_data["agent"]
            if "@type" in agent:
                affiliation_type = agent["@type"]
                if affiliation_type == "Person":
                    if "givenName" in agent:
                        self.given_name = agent["givenName"]
                    else:
                        logger.debug(f"givenName was not found in agent {agent}")
                    if "familyName" in agent:
                        self.family_name = agent["familyName"]
                    else:
                        logger.debug(f"familyName was not found in agent {agent}")
                    if "identifiedBy" in agent:
                        identifiers: List[Dict[Any]] = agent["identifiedBy"]
                        for identifier in identifiers:
                            if "@type" in identifier:
                                identifier_type = identifier["@type"]
                                value = identifier["value"]
                                if identifier_type == "Local":
                                    # these seem useless because they cannot easily be resolved to
                                    # anything and are specific to every swedish research institution
                                    self.local_identifier = value
                                elif identifier_type == "ORCID":
                                    self.orcid = value
                                else:
                                    logger.debug(f"unsupported identifier {identifier_type} in swepub agent")
                elif affiliation_type == "Organization":
                    # print("agent:")
                    # pprint(agent)
                    self.affiliations.append(SwepubAffiliation(affiliation=agent))
                else:
                    logger.debug(f"unsupported affiliation type {affiliation_type} in swepub agent")
        if "hasAffiliation" in contributor_data:
            # This affiliation is not linked to a person. Why? Bad raw_data?
            affiliations_data = contributor_data["hasAffiliation"]
            # print("hasaffiliation:")
            # pprint(affiliation)
            # exit(0)
            for affiliation_data in affiliations_data:
                affiliation = SwepubAffiliation(affiliation=affiliation_data,
                                                linked_to_person=False)
                # Unnest the subaffiliations
                if affiliation.has_subaffiliation and affiliation.subaffiliations is not None:
                    for subaffiliation_data in affiliation.subaffiliations:
                        subaffiliation = SwepubAffiliation(subaffiliation_data,
                                                           linked_to_person=False)
                        self.affiliations.append(
                            subaffiliation
                        )
                # Save memory by deleting the json raw_data
                affiliation.subaffiliations = None
                self.affiliations.append(
                    affiliation
                )
        # exit(0)

    def full_name(self):
        return f"{self.given_name} {self.family_name}"

    def export_dataframe(self):
        if len(self.affiliations) == 0:
            affiliations = None
        else:
            affiliations = self.affiliations
        data = dict(
            affiliation=affiliations,
            full_name=self.full_name(),
            orcid=self.orcid,
            local_identifier=self.local_identifier,
        )
        # The list around raw_data is needed because we have scalar values
        return pd.DataFrame(data=[data])
