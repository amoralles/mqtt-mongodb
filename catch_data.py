from datetime import datetime
from bson import json_util
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

#Configuração do MongoDB:
uri = "mongodb+srv://camorallesb:YnCix2LRJCDIJ5zk@cluster0.7kjdqt7.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
client_db = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client_db.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

def catch_data(start_time, end_time, collection_name):

    # Cria um filtro para os documentos dentro do intervalo de tempo desejado
    filter = {
        'send.timestamp': {
        "$gte": start_time.strftime("%Y-%m-%dT%H-%M-%SZ"),
        "$lte": end_time.strftime("%Y-%m-%dT%H-%M-%SZ")
        }
    }

    # Executa a consulta no banco de dados
    documents = collection.find(filter=filter)
    for i in documents:
        print(i)

    # Salva os documentos em um arquivo JSON
    filename = "files_{}_{}.json".format(start_time.strftime("%Y-%m-%dT%H-%M-%SZ"), end_time.strftime("%Y-%m-%dT%H-%M-%SZ"))
    with open(filename, "w") as file:
        for document in documents:
            json_data = json_util.dumps(document)
            file.write(json_data)
            file.write("\n")

# Acessa o banco de dados:
db = client_db["nome_do_banco_de_dados"]
collection_name = "medidas_2023-06-13"
collection = db[collection_name]

start_time = datetime(2023, 6, 13, 18, 0, 0)
end_time = datetime(2023, 6, 13, 18, 11, 0)


catch_data(start_time, end_time, collection_name) 