import os
import requests
from io import BytesIO
from fnvhash import fnv1a_64
from PIL import Image

def convert_bytes_to_jpeg(img_bytes):
    """ Convert an image (as BytesIO in-memory buffer) to JPEG. """
    # Rewind to beginning of buffer, so PIL can read it
    img_bytes.seek(0)
    # Convert to another file-like buffer but in JPEG format
    img_bytes_converted = BytesIO()
    Image.open(img_bytes).convert('RGB').save(
        img_bytes_converted, format='JPEG')
    img_bytes_converted.seek(0)
    return img_bytes_converted

def read_convert_image_from_url(url):
    """ Read the image and convert it to JPG. """
    # Get the image as in memory file-like object
    response = requests.get(url, stream=True)
    img_bytes = BytesIO(response.content)
    return convert_bytes_to_jpeg(img_bytes)

def persist_image(file_persister, url, category_name):
    """ Download the image and save. """
    bucket_name = 'slavs-lambda-scraper'
    data = read_convert_image_from_url(url)
    hashed = str(fnv1a_64(url.encode('utf-8')))
    filename = hashed + '.jpeg'
    key = os.path.join(category_name, filename)
    file_persister.set_bucket(bucket_name)
    file_persister.put_object(
        data,
        key,
        ContentType='image/jpeg',
        Metadata={'original_url': url})

    