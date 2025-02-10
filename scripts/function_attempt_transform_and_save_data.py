import pandas as pd
from pymongo.server_api import ServerApi
from pymongo.mongo_client import MongoClient
import sys, os
sys.path.append(os.path.abspath('.'))  # <-- new path appended
from my_config import MONGODB_ATLAS_CONNECTION_PASSWORD


# functions:
def connect_to_mongodb_atlas(uri):
    client = MongoClient(uri, server_api=ServerApi('1'))
    try:
        client.admin.command('ping')
        print("\nConectado ao MongoDB!")
        return client
    except Exception as e:
        print(e)

def connect_to_db(client, db_name):
    return client[db_name]

def connect_to_coll(db, coll_name):
    return db[coll_name]

# finds the first document and returns its columns' names
def columns_names(coll):
    return [name for name in coll.find({})[0].keys()]

# checks if the old column name exists, then changes it
def rename_column(coll, old_name, new_name):
    if old_name in columns_names(coll):
        coll.update_many({}, {'$rename': {old_name: new_name}})
        print(f'\natualizado de "{old_name}" para "{new_name}": \n{ columns_names(coll) }')
    else:
        print(f'\na coluna "{old_name}" não foi encontrada')

def find_distinct_column_values(coll, column_name):
    return coll.distinct(column_name)

# returns a list of data filtered by column and value, where the value can be a regex
def find_by_column_and_value(coll, column_name, value_name):
    cursor = coll.find({column_name: {'$regex': value_name}})
    return [doc for doc in cursor]

# returns a DataFrame with the data type column adjusted and the values formatted
def transform_date_format(data, column_name, old_format, new_format):
    def date_formater(format):
        if format == 'dd/mm/yy': return '%d/%m/%Y'
        if format == 'yy/mm/dd': return '%Y/%m/%d'

    df = pd.DataFrame(data)
    
    print(f'\ntipo das colunas antes da alteração:')
    df.info()
    df[column_name] = pd.to_datetime(df[column_name], format=date_formater(old_format))
    print(f'\ntipo das colunas depois da alteração:')
    df.info()
    
    df[column_name] = df[column_name].dt.strftime(date_formater(new_format))
    return df

# creates a CSV file using a DataFrame
def save_into_csv(data, path, file_name):
    try:
        data.to_csv(path+file_name, index=False)
        print(f'\narquivo "{file_name}" foi criado com sucesso!!')
    except:
        print('\nnao foi possivel criar o arquivo, verifique os dados ou o caminho informados')

# calls
# connects the mongo, db and collection
uri = f'mongodb+srv://dihparaguai:{MONGODB_ATLAS_CONNECTION_PASSWORD}@cluster-pipeline-python.mwvhw.mongodb.net/?retryWrites=true&w=majority&appName=cluster-pipeline-python-mongodbatlas'
client = connect_to_mongodb_atlas(uri)
db = connect_to_db(client, 'db_produtos')
coll = connect_to_coll(db, 'produtos')

# adjusts the columns' names
print(f'\nnomes das colunas: \n{columns_names(coll)}')
rename_column(coll, "Latitude", "lat")
rename_column(coll, "Longitude", "lon")

# extracts, transforms and saves 'livros' category into a csv file
print(f'\ncategorias de produtos existentes: {find_distinct_column_values(coll, "Categoria do Produto")}')
data = find_by_column_and_value(coll, 'Categoria do Produto', 'livros')
data = transform_date_format(data, 'Data da Compra', 'dd/mm/yy', 'yy/mm/dd')
print(data)
data = save_into_csv(data, 'data/', 'produtos_categoria_livros_with_script.csv')

# extracts, transforms and saves 'Data da Compra' greater than 2020 into a csv file
data = find_by_column_and_value(coll, 'Data da Compra', '/202[1-9]')
data = transform_date_format(data, 'Data da Compra', 'dd/mm/yy', 'yy/mm/dd')
print(data)
data = save_into_csv(data, 'data/', 'produtos_data_compra_maior_que_2020_with_script.csv')

# close the mongo's connections
client.close()