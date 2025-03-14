from pymongo.server_api import ServerApi 
from pymongo.mongo_client import MongoClient
import requests
import pandas as pd

# Imports that contain the password to connect to MongoDB Atlas
import os
from dotenv import load_dotenv
load_dotenv()


def connect_to_mongodb(uri):

    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
    return client


def extract_api_data(url):
    try:
        return requests.get(url, timeout=10).json()['results']
    except:
        return []


psswrd = os.getenv('MONGODB_ATLAS_PASSWORD')
uri = f"mongodb+srv://dihparaguai:{psswrd}@cluster-pipeline-python.mwvhw.mongodb.net/?retryWrites=true&w=majority&appName=cluster-pipeline-python-mongodbatlas"
client = connect_to_mongodb(uri)

# Creates both database and collection
db_name = 'db_usuarios'
db = client[db_name]
coll = db['usuarios']

url = 'https://randomuser.me/api/?results=500'
api_data = extract_api_data(url)
print(f'\nprimeiro registro da api:\n{api_data[1]}')

# inserts api data into collection
coll.insert_many(api_data)
print(f'\nqtd de documentos na base:\n{coll.count_documents({})}')

# filters distinct genders
print(f'\ngeneros disponiveis na coleção:\n{coll.distinct("gender")}')

# lists users who only have 'US' as 'nat'
print(f'\nusuarios com nacionalidade "US":\n{ list( coll.find( {"nat": "US"}, {"name.first": 1, "nat": 1, "_id": 0} ) ) }')

# lists users' names only that contain 'John'
# '$options: i' is used to not compare upper or lower case
print(f'\nusuario que contem "jhon" no primero nome:\n{ list( coll.find( {"name.first": {"$regex": "John", "$options": "i"}}, {"name": 1, "_id": 0} ) ) }')

# adds a new key only to female gender
coll.update_many({'gender': 'female'}, {'$set': {'status': 'verified'}})
print(f'\nusuarios que possuem "status" de acordo com o "genero":\n{ list( coll.find( {}, {"gender": 1, "status": 1, "name.first": 1, "_id": 0} ).limit(5) ) }')

# renames some columns
coll.update_many({}, {'$rename': {'gender': 'genero', 'registered.date': 'registro.data_registro', 'name.first': 'nome.primeiro_nome', 'name.last': 'nome.ultimo_nome'}})
print(f'\nnome das colunas atualizadas:\n{ list( coll.find( {}, {"registro.data_registro": 1, "nome.primeiro_nome": 1, "nome.ultimo_nome": 1, "_id": 0} ).limit(5) ) }')

# creates a dataframe specific spefic columns
df = pd.DataFrame(coll.find({}, {"registro.data_registro": 1, "nome.primeiro_nome": 1, "genero": 1}))

# due the to data being in a dictionary, lambda is used to extract the needed data
df['nome'] = df['nome'].apply(lambda x: x['primeiro_nome'])
df['registro'] = df['registro'].apply(lambda x: x['data_registro'])
df['registro'] = df['registro'].apply(lambda x: x[:10])
print('\ntabela com alguns dados da coleção:\n', df.head(3))

# creates a csv file using dataframe data
csv_path = '__exercises__/aula03/ex02/usuarios_transformados.csv'
df.to_csv(csv_path, index=False)

# drops the collection and closes the connection to mongodb atlas
coll.drop()
client.close()