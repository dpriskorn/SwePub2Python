import logging
import os
import time
import zipfile
from pathlib import Path

# This script is intended to be run on the WMC Kubernetes cluster

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

# This file is huge. 1.3GB gzipped json
name = f'{Path.home()}/WikidataMLSuggester/swepub-deduplicated.zip'

# There is a lot of bloat in their choice of specification.
# E.g. titles of all the UKÄ codes could have been left out
# and put into a Wikibase graph database instead and just linked
# That would have saved a lot of space and bandwidth.

# Suggestions for improvements of the data models:
# 1) Add language codes to titles just as you do for summaries.

start = time.time()
# This probably read the whole thing into memory which is not ideal...
with zipfile.ZipFile(name) as z:
    for filename in z.namelist():
        if not os.path.isdir(filename):
            # read the file
            with z.open(filename) as f:
                count = 0
                for line in f:
                    count += 1
                    if count % 100000 == 0:
                        print(f"count:{count} duration:{round(time.time() - start)}s", flush=True)
end = time.time()
print(f"total duration: {round(end - start)}s")
