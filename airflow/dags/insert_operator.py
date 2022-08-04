from airflow.models.baseoperator import BaseOperator
from mongo_inserter import MongoInserter

class MongoInserterOperator(BaseOperator):
    def __init__(self, inserted_file: str, conn_id: str, collection_name: str, db_name : str, insert_with_update: bool, **kwargs) -> None:
        super().__init__(**kwargs)
        self.inserted_file = inserted_file
        self.conn_id = conn_id
        self.collection_name = collection_name
        self.db_name = db_name
        self.insert_with_update = insert_with_update
        

    def execute(self, context):
        mongo_inserter = MongoInserter(self.inserted_file, self.conn_id)
        mongo_inserter(self.collection_name, self.db_name, self.insert_with_update)