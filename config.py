version = "0.1-dev2"
wd_prefix = "http://www.wikidata.org/entity/"
user = "User:So9q" # change to your username on Wikidata
user_agent = f"SwePub2Python/{version} (User:{user})"

# Settings
# Note: Parsing of identifiers is always done.
# The settings below increase the processing time considerably
# but also extracts more valuable information from the dataset.
detect_language_of_abstract = False
lookup_languages_in_wd = True
lookup_topics_in_wd = False
parse_contributors = True
parse_titles = True
parse_abstracts = True
parse_subjects = True
sleep_after_topic_match = 0 # seconds