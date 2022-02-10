# SwePub2Python

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

If this was done for SwePub the filesize would probably shrink considerably.

This is also good for the environment, as it keeps processing time to a minimum for consumers.

Suggestions for improvements of the dataset:
1) Add language codes to titles just as you do for summaries.
2) Publish a specification of the format and validate that you follow it

## What I learned from this project
* Good data starts with a clear and well-thought-out specification. 
  It results in a minimum of guesswork for the consumer as opposed to 
  a hairy mess of objects with unclear relations like in this case.
* I ran into Kubernetes errors with this script. It is still unknown
  what causes them. I really prefer to do my own computing whenever 
  possible so I can control the whole environment. Kubernetes introduces
  complexity.

## License 
GPLv3+