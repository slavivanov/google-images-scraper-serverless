import os
import sys
here = os.path.dirname(os.path.realpath(__file__))
# For local debugging only
sys.path.append(os.path.join(here, 'dev_local_include')) 
sys.path.append(os.path.join(here, 'vendor')) 

from lib.gimages_scraper import GImagesScraper
from lib.persist_gimages import persist_gimages, mark_query_retrieved
from lib.util import process_exception

def process_record(record):
    assert 'dynamodb' in record
    assert 'NewImage' in record['dynamodb']
    new_insert = record['dynamodb']['NewImage']
    assert 'query' in new_insert
    assert 'S' in new_insert['query']
    query = new_insert['query']['S']
    if 'max_images' in new_insert:
        max_images = new_insert['max_images']['N']
    else:
        max_images = 1000
    if 'photos_only' in new_insert:
        photos_only = new_insert['photos_only']['BOOL']
    else:
        photos_only = False
        
    return query, max_images, photos_only

def scrape(event, context):
    print(event)
    if not 'Records' in event:
        return
    try:
        assert len(event['Records']) == 1

        record = event['Records'][0]
        if record['eventName'] != 'INSERT':
            return
        scraper = GImagesScraper()
        query, max_images, photos_only = process_record(record)
        urls = scraper.get_images(query, max_images, photos_only)
        persist_gimages(query, urls)
        mark_query_retrieved(query, len(urls))
    except Exception as e:
        process_exception(e)