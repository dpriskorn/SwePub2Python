# Usage: chmod +x this script and run like this "./create_kubernettes_article_job_and_watch_the_log.sh 1"
# Increment the job number yourself each time
toolforge-jobs run swepub-article-job$1 --image tf-python39 --command "pip install -r ~/WikidataMLSuggester/requirements.txt && python3 ~/WikidataMLSuggester/extract_articles_from_swepub.py"
watch tail ~/swepub-article-job$1*
