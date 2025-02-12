import sys, os
sys.path.append(os.path.abspath('.'))
from my_config import MONGODB_ATLAS_CONNECTION_PASSWORD

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = f"mongodb+srv://dihparaguai:{MONGODB_ATLAS_CONNECTION_PASSWORD}@cluster-pipeline-python.mwvhw.mongodb.net/?retryWrites=true&w=majority&appName=cluster-pipeline-python-mongodbatlas"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)