import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from lib.dynamo_db_connector import DynamoDBConnector


class URLPersistenceInterface(ABC):
    """ Abstract class for persisting Queries."""
    @abstractmethod
    def __init__(self):
        raise NotImplementedError

    @abstractmethod
    def put(self, value):
        """ Persist an URL """
        raise NotImplementedError
    
    @abstractmethod
    def get(self, value):
        """ Retrieve an URL by value. """
        raise NotImplementedError
    
    def delete(self, value):
        """ Delete an URL by value."""
        raise NotImplementedError

class URLPersistenceDynamo(URLPersistenceInterface, DynamoDBConnector):
    def __init__(self):
        self.key = 'url'
        table_name = os.environ['url_persistence_table_name']
        self._table = self._setup_db(table_name)
        self._batch_writer = self._table.batch_writer()
        
    def put(self, value):
        time_string = datetime.now(timezone.utc).isoformat()

        self._table.put_item(Item={
            self.key: value,
            'created_at': time_string})

    def put_batch(self, value, **extra_data):
        time_string = datetime.now(timezone.utc).isoformat()
        default_data = {
            self.key: value,
            'created_at': time_string }
        item = {**default_data, **extra_data}
        self._batch_writer.put_item(Item=item)
        
    def get(self, value):
        """ Get entry by the given value 
        using the default key. """
        return self.get_by(self.key, value)
     
    def delete(self, value, batch=False):
        """ Delete entry with the given 'value' 
        using the default key.
        """
        if batch:
            delete_client = self._batch_writer
        else:
            delete_client = self._table
        delete_client.delete_item(Key={
            self.key: value})

    def flush_batch(self):
        self._batch_writer.__exit__(None, None, None)



