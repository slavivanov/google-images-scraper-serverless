import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from time import sleep        
        
class DynamoDBConnector():
    def _setup_db(self, table_name):
        self._db = boto3.resource('dynamodb')
        self._db_client = boto3.client('dynamodb')
        return self._get_create_table(table_name)

    def _get_create_table(self, table_name):
        table = self._db.Table(table_name)
        try:
            table_status = table.table_status
            print('Storage found.')
            return table
        except self._db_client.exceptions.ResourceNotFoundException as e:
            # No table found, so create it
            # return self._create_storage(table_schema)

            # Currently the table is created via serverless.
            # If it's missing, raise an exception
            raise e
            
    def _create_storage(self, table_schema):
        """ Create the table.
        """
        print('Storage not found, creating table.')
        table = self._db.create_table(**table_schema)
        print('Waiting for table to be created.')
        while table.table_status != 'ACTIVE':
            print('.', end='')
            sleep(0.1)
            table.reload()
        print('Storage created.')
        return table
        
    def get_by(self, key, value):
        try:
            response = self._table.get_item(
                Key={
                    key: value,
                }
            )
            # If we found records
            if 'Item' in response:
                return response['Item']
            else:
                return False
        except ClientError as e:
            print(e.response['Error']['Message'])
            return False

    def get_all(self):
        response = self._table.scan()
        if 'Items' in response:
            return response['Items']
        else:
            return False
