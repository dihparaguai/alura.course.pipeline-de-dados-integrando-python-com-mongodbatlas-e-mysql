from pymongo.server_api import ServerApi
from pymongo.mongo_client import MongoClient
import requests

# Imports that contain the password to connect to MongoDB Atlas
import os
from dotenv import load_dotenv
load_dotenv()

psswrd = os.getenv('MONGODB_ATLAS_PASSWORD')
uri = f"mongodb+srv://dihparaguai:{psswrd}@cluster-pipeline-python.mwvhw.mongodb.net/?retryWrites=true&w=majority&appName=cluster-pipeline-python-mongodbatlas"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

# Creates both database and collection
db_name = 'db_produtos'
db = client[db_name]
coll = db['produtos']

# Inserts manual docs into collection
docs = [{'produto_nome': 'celular', 'categoria': 'eletronico', 'preco': 4000}, {
    'produto_nome': 'cafeteira', 'categoria': 'eletrodomestico', 'preco': 300}]
coll.insert_many(docs)

# Counts how many docs exist in the collection
## oll_docs_qtd = len([doc for doc in coll.find({})])
coll_docs_qtd = coll.count_documents({}) # other way to do the same thing
print(f'\ntotal de documentos na coleção: {coll_docs_qtd}')

# Performs a connection with the api and extracts its data
url = 'https://fakestoreapi.com/products'
try:
    req = requests.get(url)
    if req.status_code == 200:
        docs = req.json()
except:
    req = None

# Inserts and counts how many api docs were inserted into collection
inserted_docs = coll.insert_many(docs)
print(
    f'\ntotal de documentos inseridos na inseridos: {len(inserted_docs.inserted_ids)}')

# Counts how many docs the collection has
## coll_docs_qtd = len([doc for doc in coll.find({})])
coll_docs_qtd = coll.count_documents({}) # other way to do the same thing
print(f'\ntotal de documentos na coleção: {coll_docs_qtd}')

# Shows the fisrt two docs from the collection
## first_2_docs = [doc for doc in coll.find({}, {'produto_nome': 1}).limit(2)]
first_2_docs = list(coll.find({}, {'produto_nome': 1}).limit(2)) # other way to do the same thing
print(f'\nprimeiros 2 produtos da base de dados:\n{first_2_docs}')

# Selects and deletes the second doc, then show again the first two docs from the collection
doc_id = first_2_docs[1]['_id']
coll.delete_one({'_id': doc_id})
## first_2_docs = [doc for doc in coll.find({}, {'produto_nome': 1}).limit(2)]
first_2_docs = list(coll.find({}, {'produto_nome': 1}).limit(2)) # other way to do the same thing
print(
    f'\nprimeiros 2 produtos da base de dados depois da deleção:\n{first_2_docs}')

# Drops the database and closes the MongoDB Atlas connection
client.drop_database(db_name)
client.close()
