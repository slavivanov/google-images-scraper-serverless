import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from lib.dynamo_db_connector import DynamoDBConnector


class QueryPersistenceInterface(ABC):
    """ Abstract class for persisting Queries."""
    @abstractmethod
    def __init__(self):
        raise NotImplementedError

    @abstractmethod
    def put(self, query):
        """ Put a query into the DB. """
        raise NotImplementedError
    
    @abstractmethod
    def get(self, value):
        """ Retrieve a query by value. """
        raise NotImplementedError

    @abstractmethod 
    def delete(self, value):
        """ Delete a query by value. """
        raise NotImplementedError

    @abstractmethod 
    def update(self, value, update_attributes):
        """ Update a query by value, given a dict of attributes to update."""
        raise NotImplementedError


class QueryPersistenceDynamo(QueryPersistenceInterface, DynamoDBConnector):
    def __init__(self):
        self.key = 'query'
        table_name = os.environ['query_persistence_table_name']
        self._table = self._setup_db(table_name)
        self._batch_writer = self._table.batch_writer()

    def put(self, query):
        time_string = datetime.now(timezone.utc).isoformat()

        self._table.put_item(Item={
            self.key: query,
            'created_at': time_string})

    def get(self, value):
        return self.get_by(self.key, value)

    def delete(self, value):
        """ Delete a query with
            the given 'value'.
        """
        self._table.delete_item(Key={
            self.key: value})

    def update(self, key_value, update_attributes):
        """ Update record in table.
        Args:
            key_value: Key identifying the item to update.
            update_attributes: A dict with {attribute_name: value}
                to add/update to item.
        """
        # Create expression to update the table
        first = True
        expression_attribute_values = {}
        update_expression = 'set '
        for attribute, value in update_attributes.items():
            if first:
                first = False
            else:
                update_expression += ', '
            update_expression += '{} = :{}'.format(attribute, attribute)
            expression_attribute_values[':'+attribute] = value
        # Perform the expression
        self._table.update_item(
            Key={self.key: key_value},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values
        )
