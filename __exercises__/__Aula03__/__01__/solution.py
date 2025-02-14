import os, sys
sys.path.append(os.path.abspath('.'))
from my_config import MONGODB_ATLAS_CONNECTION_PASSWORD

from pymongo.server_api import ServerApi
from pymongo.mongo_client import MongoClient
import requests
import pandas as pd

# connects to mongodb atlas and extract api data
def connect_client(uri):
    client = MongoClient(uri, server_api=ServerApi('1'))
    try:
        client.admin.command('ping')
        print("\nPinged your deployment. You successfully connected to MongoDB!")
        return client
    except Exception as e:
        print(e)

def connect_collection(client, db_name, colletion_name):
    db = client[db_name]
    return db[colletion_name]

def extract_data_api(url):
    try:
        return requests.get(url, timeout=10).json()
    except:
        return None

# step1: connects to mongodb atlas, db and collection
uri = f'mongodb+srv://dihparaguai:{MONGODB_ATLAS_CONNECTION_PASSWORD}@cluster-pipeline-python.mwvhw.mongodb.net/?retryWrites=true&w=majority&appName=cluster-pipeline-python-mongodbatlas'
client = connect_client(uri)
collection = connect_collection(client, 'db_produtos', 'produtos')
print(f'qtd dados na colecao: {collection.count_documents({})}')

# step2: extracts data from the api
url_api = 'https://labdados.com/produtos'
data_api = extract_data_api(url_api)

# step3: inserts the api data as documents into mongodb atlas 
print(f'\nqtd de dados inseridos: {len( collection.insert_many(data_api).inserted_ids )}')
print(f'primeiro produto: {collection.find_one()}')
print(f'qtd dados na colecao: {collection.count_documents({})}')

# step4: shows the first ten products that have 'eletronicos' as category
print(f'\ncategorias da colecao: {collection.distinct("Categoria do Produto")}')
query = {'Categoria do Produto': 'eletronicos'}
project = {'Produto': 1, 'Categoria do Produto': 1, '_id': 0}
print(f'primeiros 10 produtos da "Categoria do Produto": { [doc for doc in collection.find(query, project).limit(10)] }')

# step5: shows the first ten products that have 'Smart' as part of their name
query = {'Produto': {'$regex': 'Smart'}} # case sensitive
project = {'Produto': 1, '_id': 0}
print(f'\nprimeiros 10 produtos que contem "Smart" no nome: { [doc for doc in collection.find(query, project).limit(10)] }')

# step6: updates the category from 'eletronicos' to 'gadgets'
print(f'\ncategorias antes de atualizar: {collection.distinct("Categoria do Produto")}')
query = {'Categoria do Produto': 'eletronicos'}
update = {'$set': {'Categoria do Produto': 'gadgets'}}
collection.update_many(query, update)
print(f'categorias depois de atualizar: {collection.distinct("Categoria do Produto")}')

# step7: updates some column names
print(f'\nnome das colunas antes de atualizar: {collection.find_one().keys()}')
update = {'$rename': {'Produto': 'produto_nome', 'Categoria do Produto': 'categoria_produto', 'Data da Compra': 'data_compra'}}
collection.update_many({}, update)
print(f'nome das colunas antes de atualizar: {collection.find_one().keys()}')

# step8: creates a dataframe using mongodb data and changes the type and format of the date
project = {'produto_nome': 1, 'categoria_produto': 1, 'data_compra': 1, '_id': 0}
df = pd.DataFrame([doc for doc in collection.find({}, project)])
print(f'\nformato das datas antes de atualizar: ')
print(df)
df['data_compra'] = pd.to_datetime(df['data_compra'], format='%d/%m/%Y') # needs to change the data type to date type first
df['data_compra'] = df['data_compra'].dt.strftime('%Y/%m/%d')
print(df)

# step9: saves the dataframe in a csv file
df.to_csv('__exercises__/__Aula03__/__01__/produtos_transformados.csv')

collection.drop()
client.close()