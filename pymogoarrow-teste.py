from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime
import os
import pyarrow
import pymongo
import bson
import pymongoarrow.monkey
from pymongoarrow.api import Schema

uri = "mongodb+srv://camorallesb:YnCix2LRJCDIJ5zk@cluster0.7kjdqt7.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
#client_db = os.environ['uri']

client_db = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client_db.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

pymongoarrow.monkey.patch_all()

db= client_db['BancoTeste2']
collection = db.get_collection('medidas_2023-06-13')

print(collection.find_pandas_all(
    {},
    schema=Schema({
        'send.timestamp': pyarrow.timestamp('ms'),
    })
).head())