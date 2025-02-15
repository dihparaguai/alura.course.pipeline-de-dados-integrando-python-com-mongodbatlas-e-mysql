# script to simplify the notebook
import sys
import os
import requests
sys.path.append(os.path.abspath('.'))
from my_config import MONGODB_ATLAS_CONNECTION_PASSWORD
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


class MongoDBAtlasInsertDataFromAPI():

    def __init__(self, mongo_uri, db_name, collection_name, api_url):
        self.api_url = api_url
        self.db_name = db_name
        self.collection_name = collection_name
        self.__client = MongoClient(mongo_uri, server_api=ServerApi('1'))
        self.__connection_test()
        self.__delete_database()
        self.__insert_many()
        self.count_data_api
        self.count_data_insert_to_db
        self.count_all_data_db

    def __connection_test(self):
        try:
            self.__client.admin.command('ping')
            print("conexão com mongodb atlas com sucesso!")
        except Exception as e:
            print(e)

    def __insert_many(self):
        db = self.__client[self.db_name]
        collection = db[self.collection_name]

        docs = collection.insert_many(self.__extract_data_api())
        self.count_data_insert_to_db = len(docs.inserted_ids)
        self.count_all_data_db = collection.count_documents({})

        print(
            f'quantidade de documentos adicionados na base dados: {self.count_data_insert_to_db}')
        print(
            f'quantidade total de documentos na base dados: {self.count_all_data_db}')

        self.__client.close()

    def __extract_data_api(self):
        try:
            req = requests.get(self.api_url)
            data = req.json()
            self.count_data_api = len(data)
            print('extração de dados da api com sucesso!')
            print(f'quantidade de dados da api: {self.count_data_api}')
        except:
            data = None
            print(req.status_code)
        return data

    def __delete_database(self):
        self.__client.drop_database(self.db_name)
        print(f'base de dados "{self.db_name}" deletada')


mongo_uri = f'mongodb+srv://dihparaguai:{MONGODB_ATLAS_CONNECTION_PASSWORD}@cluster-pipeline-python.mwvhw.mongodb.net/?retryWrites=true&w=majority&appName=cluster-pipeline-python-mongodbatlas'
api_url = 'https://labdados.com/produtos'

db = MongoDBAtlasInsertDataFromAPI(
    mongo_uri=mongo_uri,
    db_name='db_produtos',
    collection_name='produtos',
    api_url=api_url)