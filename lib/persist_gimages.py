import os
from PIL import Image
from io import BytesIO
import base64
import requests
import boto3
from timeit import default_timer as timer
from lib.url_persistence_dynamo import URLPersistenceDynamo
from lib.s3_file_persister import S3FilePersister
from lib.fnv32a_hash import fnv32a_hash

def persist_gimages(query, urls):
    """ Same Image URLs to Dynamo DB for later retrieval.
        Save Inline images to S3."""
    func_start_time = timer()
    url_persister = URLPersistenceDynamo()
    bucket_name = 'slavs-lambda-scraper'
    category_name = query.lower().replace(' ','-')
    file_persister = S3FilePersister(bucket_name)
    print('Persisting found images')
    num_embedded = 0
    num_urls = 0
    # Write urls in batches
    for index, url in enumerate(urls):
        if not url:
            continue
        start_time = timer()
        if url[:5] == 'data:':
            # base64 encoded image, save directly to S3
            image_type, image_extension, data = prepare_base64_image(url)
            retrieval_method = 'embedded'
            num_embedded += 1
            # Save the data to S3
            hashed = str(fnv32a_hash(str(url[:1000])))
            filename = hashed + '.' + image_extension
            key = os.path.join(category_name, filename)
            try:
                file_persister.put_object(data, key, ContentType=image_type)
            except Exception as e:
                print('Could not save item {}. Error: {}'.format(
                    index, str(e)))
        elif url[:4] == 'http':
            # download the image later, persist in Dynamo DB
            url_persister.put_batch(url, category_name=category_name)
            retrieval_method = 'download later'
            num_urls += 1 
        elapsed_time = timer() - start_time
    url_persister.flush_batch()
    mark_query_retrieved(query, num_embedded + num_urls)
    print('Number of embedded images: {}'.format(num_embedded))
    print('Number of links: {}'.format(num_urls))
    print('Done in {:.2f}s'.format(timer() - func_start_time))

def prepare_base64_image(data):
    # Currently all images are JPEG
    image_type = 'image/jpeg'
    # From 'image/jpeg' to 'jpeg'
    image_extension = image_type.replace('image/', '')
    if data[:5] == 'data:':
        # Data is like 
        # "data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAD..."
        # figure the image type
        image_type = data[data.find("data:"):data.find(";base64")].replace('data:', '')
        # The actual base64 encoded image
        data = data[data.find(",")+1:]
    decoded_image = base64.b64decode(data)
    return image_type, image_extension, data
        
def mark_query_retrieved(query, num_items):
    """ Mark the query in Dynamo DB as retrieved. """
    from lib.query_persistence_dynamo import QueryPersistenceDynamo
    query_persister = QueryPersistenceDynamo()
    from datetime import datetime, timezone
    time_string = datetime.now(timezone.utc).isoformat()
    query_persister.update(query, {'retrieved_at': time_string, 
                                   'items_retrieved': num_items})