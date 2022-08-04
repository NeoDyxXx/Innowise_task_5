from mongo_hook import MongoHook
import pandas as pd
import numpy as np

class MongoInserter(MongoHook):
    default_conn_name = 'mongo_default'

    def __init__(self, inserted_file: str, conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(conn_id, *args, **kwargs)
        self.inserted_file = inserted_file
    
    def __call__(self, mongo_collection: str, db_name: str, insert_with_update: bool = True):
        inserted_data = pd.read_csv(self.inserted_file)
        if insert_with_update:
            for index, data in inserted_data.iterrows():
                doc = {
                    "reviewId": data['reviewId'],
                    'userName': data['userName'],
                    'userImage': data['userImage'],
                    'content': data['content'],
                    'score': data['score'],
                    'thumbsUpCount': data['thumbsUpCount'],
                    'reviewCreatedVersion': data['reviewCreatedVersion'],
                    'at': data['at'],
                    'replyContent': data['replyContent'],
                    'repliedAt': data['repliedAt']
                }
                
                if super().find(mongo_collection=mongo_collection, \
                    query={"reviewId": data['reviewId']}, find_one=True, mongo_db=db_name) == None:
                    super().insert_one(mongo_collection, doc, db_name)
                    if index % 1000 == 0:
                        print('Insert ' + str(index))
                else:
                    super().replace_one(mongo_collection, doc, {"reviewId": data['reviewId']}, db_name)
                    if index % 1000 == 0:
                        print('Replace ' + str(index))
        else:
            docs = inserted_data.to_dict('records')
            super().insert_many(mongo_collection, docs, db_name)

            