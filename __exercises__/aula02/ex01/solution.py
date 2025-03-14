# imports password to connect to mongodb atlas
import sys, os
sys.path.append(os.path.abspath('.'))
from my_config import MONGODB_ATLAS_CONNECTION_PASSWORD

from pymongo.server_api import ServerApi
from pymongo.mongo_client import MongoClient
import requests

# connects to mongodb atlas
def connect_client(uri):
    client = MongoClient(uri, server_api=ServerApi('1'))
    try:
        client.admin.command('ping')
        print("\nPinged your deployment. You successfully connected to MongoDB!")
        return client
    except Exception as e:
        print(e)

# connects to both the db and collection
def connect_collection(client, db_name, coll_name):
    db = client[db_name]
    return db[coll_name]

# extracts specific data from the api
def extract_api_data(url):
    list = []
    try:
        req = requests.get(url, timeout=10).json()
    except:
        print(f'nao foi possivel extrair os dados')
        req = None

    for dict in req:
        dict_temp = {}
        for key, value in dict.items():
            if key in {'id', 'name'}:
                dict_temp[key] = value
        list.append(dict_temp)

    return list

# step1: connects to mongodb atlas, db and collection
uri = f'mongodb+srv://dihparaguai:{MONGODB_ATLAS_CONNECTION_PASSWORD}@cluster-pipeline-python.mwvhw.mongodb.net/?retryWrites=true&w=majority&appName=cluster-pipeline-python-mongodbatlas'
client = connect_client(uri)
collection = connect_collection(client, 'db_usuarios', 'usuarios')
print(f'\nqtd dados na colecao: {collection.count_documents({})}')

# step2: adds some documents into the collection
docs = [{'nome': 'diego', 'idade': 18, 'cidade': 'carapicuiba'}, 
    {'nome': 'rodrigo', 'idade': 18, 'cidade': 'carapicuiba'}]
docs = collection.insert_many(docs)
print(f'\nqtd de dados inseridos: {len(docs.inserted_ids)}')
print(f'qtd dados na colecao: {collection.count_documents({})}')

# step3: extracts data from the api
url_api = 'https://jsonplaceholder.typicode.com/users/'
req = extract_api_data(url_api)
print(f'\ndados da api: {req}')
print(f'\nqtd de dados da api: {len(req)}')

# step3: inserts all api data into the collection
req = collection.insert_many(req)
print(f'qtd de dados inseridos: {len(req.inserted_ids)}')
print(f'qtd dados na colecao: {collection.count_documents({})}')

# step4: shows e deletes the first document
first_doc_id = collection.find_one()['_id']
collection.delete_one( {"_id": first_doc_id} )
print(f'\nprimeiro doc da colecao deletado: {first_doc_id}')
print(f'novo primeiro doc da colecao: {collection.find_one()["_id"]}')
print(f'qtd dados na colecao: {collection.count_documents({})}')

# drops the collection and closes the connection to mongodb atlas
collection.drop()
client.close()