#SwePub2Python

This tries to extract SwePub into Python objects to work on the data using Pandas

## Issues in SwePub


There is a lot of bloat in their choice of specification.
E.g. titles of all the UKÄ codes could have been left out
and put into a Wikibase graph database instead and just linked
That would have saved a lot of space and hassle for consumers. 
The same could be done with all the language handling. 
Here SwePub could simply link to Wikidata because all languages 
in the world are already modeled there and many data consumers 
already added support for WD into their workflows.

Suggestions for improvements of the data models:
1) Add language codes to titles just as you do for summaries.
2) Publish a specification of the format and validate that you follow it

## License 
GPLv3+