# script to simplify the notebook
import sys
import os
import requests
sys.path.append(os.path.abspath('.'))
from my_config import MONGODB_ATLAS_CONNECTION_PASSWORD
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# ETL functions
def mongo_connect(uri):
    client = MongoClient(uri, server_api=ServerApi('1'))
    try:
        client.admin.command('ping')
        print("conexão com mongodb atlas com sucesso!")
    except Exception as e:
        print(e)        
    return client

def create_and_drop_db_connection(client, db_name):
    client.drop_database(db_name)
    return client[db_name]

def create_coll_connection(db, coll_name):
    return db[coll_name]

def extract_api_data(url):
    return requests.get(url).json()

def insert_api_data_on_coll(coll, api_data):
    return coll.insert_many(api_data)

def n_docs_inserted(inserted_docs):
    return len(inserted_docs.inserted_ids)

def n_docs_on_coll(coll):
    return coll.count_documents({})

# calls functions
mongo_uri = f'mongodb+srv://dihparaguai:{MONGODB_ATLAS_CONNECTION_PASSWORD}@cluster-pipeline-python.mwvhw.mongodb.net/?retryWrites=true&w=majority&appName=cluster-pipeline-python-mongodbatlas'
api_url = 'https://labdados.com/produtos'

# creates connection and deletes all data
client = mongo_connect(mongo_uri)
db = create_and_drop_db_connection(client, 'db_produtos_3')
coll = create_coll_connection(db, 'produtos')

data = extract_api_data(api_url)
print(f'qtd dados api: {len(data)}')

inserted_docs = insert_api_data_on_coll(coll, data)
print(f'qtd dados inseridos: {n_docs_inserted(inserted_docs)}')

print(f'qtd dados na colecao: {n_docs_on_coll(coll)}')
    
# closes the MongoDB connection
client.close() 

