import boto3

class S3FilePersister(object):
    def __init__(self, bucket_name=None):
        self.s3 = boto3.client('s3')
        self.s3_res = boto3.resource('s3')
        if bucket_name:
            self.set_bucket(bucket_name)

    def set_bucket(self, bucket_name):
        self.bucket_name = bucket_name

    def put(self, key, filename, **kwargs):
        try:
            self.s3_res.meta.client.upload_file(
                filename, self.bucket_name, key, **kwargs)
        except self.s3.exceptions.NoSuchBucket:
            self._create_bucket(self.bucket_name)

    def put_object(self, data, key, **kwargs):
        if 'ContentType' not in kwargs:
            kwargs['ContentType']='image/jpeg', 
        if 'ACL' not in kwargs:
            kwargs['ACL']='public-read'
        try:
            self.s3_res.Bucket(self.bucket_name).put_object(
                Key=key, 
                Body=data, 
                **kwargs)
        except self.s3.exceptions.NoSuchBucket:
            self._create_bucket(self.bucket_name)

    def _create_bucket(self, bucket_name):
        
        self.s3.create_bucket(
            Bucket=bucket_name, 
            ACL='private', 
            CreateBucketConfiguration={'LocationConstraint': 'EU'})
        
        bucket = self.s3_res.Bucket(bucket_name)
        bucket.wait_until_exists()