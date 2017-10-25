import os
import sys
here = os.path.dirname(os.path.realpath(__file__))
# For local debugging only
sys.path.append(os.path.join(here, 'dev_local_include')) 
sys.path.append(os.path.join(here, 'vendor')) 


from timeit import default_timer as timer
from lib.download_image import persist_image
from lib.s3_file_persister import S3FilePersister
from lib.url_persistence_dynamo import URLPersistenceDynamo
from lib.util import process_exception


def process_dynamo_record(record):
    """ Get the URL and category from a DynamoDB record. """
    assert 'dynamodb' in record
    assert 'NewImage' in record['dynamodb']
    new_insert = record['dynamodb']['NewImage']
    assert 'url' in new_insert
    assert 'S' in new_insert['url']
    url = new_insert['url']['S']
    assert 'category_name' in new_insert
    assert 'S' in new_insert['category_name']
    category_name = new_insert['category_name']['S']

    return url, category_name

def download(event, context):
    """ The main func, triggered by a Dynamo DB Insert.
        Downloads several images.
     """
    start_time = timer()
    print(event)
    if not 'Records' in event:
        return
    file_persister = S3FilePersister()
    url_persister = URLPersistenceDynamo()

    downloaded_images_count = 0
    for record in event['Records']:
        try:
            # We want to process only Inserts
            if record['eventName'] != 'INSERT':
                continue
            url, category_name = process_dynamo_record(record)
            persist_image(file_persister, url, category_name)
            # Delete the item from Dynamo
            url_persister.delete(url, batch=True)
            downloaded_images_count += 1
        except Exception as e:
            process_exception(e)
    try:
        # The last batch needs to be flushed.
        url_persister.flush_batch()
    except Exception as e:
        process_exception(e)

    if downloaded_images_count > 0:
        elapsed_time = timer() - start_time
        print('{} items saved in {:.2f}s'.format(
            downloaded_images_count,
            elapsed_time))
    