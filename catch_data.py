from datetime import datetime
from bson import json_util
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import infos_sensiveis
import pytz
import paho.mqtt.client as paho
from paho import mqtt

#Configuração do MongoDB:
uri = infos_sensiveis.uri_mdb

# Create a new client and connect to the server
client_db = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client_db.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

# Converte a Data e Hora do horário BR para o horário Padrão UTC
def br_to_utc(dt):
    br_timezone = pytz.timezone('America/Sao_Paulo')
    utc_timezone = pytz.utc
    dt_br = br_timezone.localize(dt)
    dt_utc = dt_br.astimezone(utc_timezone)
    return dt_utc

def catch_data(start_time, end_time, collection):
    # Muda o timezone de BR para UTC
    start_time_utc = br_to_utc(start_time)
    end_time_utc = br_to_utc(end_time)

    #Converte as datas para strings no formato ISO8601
    start_time_iso = start_time_utc.isoformat() + "Z"
    end_time_iso = end_time_utc.isoformat() + "Z"    

    # Cria um filtro para os documentos dentro do intervalo de tempo desejado
    filter = {"send.timestamp": {"$gte": start_time_iso, "$lte": end_time_iso}}
    #filter = {"send.timestamp": {"$gte": "2023-06-19T14:45:00Z", "$lte": "2023-06-19T14:49:00Z"}}
    print("Filtro: ", filter)
    
    # Executa a consulta no banco de dados
    documents = list(collection.find(filter))
    print(len(documents)," arquivos encontrados.")

    # Salva os documentos em um arquivo JSON
    filename = "files_{}_{}.json".format(start_time.strftime("%Y-%m-%dT%H-%M-%SZ"), end_time.strftime("%Y-%m-%dT%H-%M-%SZ"))
    with open(filename, "w") as file:
        for document in documents:
            json_data = json_util.dumps(document)
            file.write(json_data)
            file.write("\n")
    
    print("Arquivo salvo com sucesso.")

#Definição do db e collection
db= client_db['BancoTeste2']
today = datetime.utcnow().date()
collection_today = f"medidas_{today}"

# Configurações do MQTT Broker
broker_address = infos_sensiveis.broker_address
port = infos_sensiveis.port_broker
topic_request = "+/request"
topic_response = "+/response"

# # Configurações do dispositivo
# app_id = topic.split("/")[0]
# service_id = topic.split("/")[1]

# Função de callback chamada quando a conexão ao broker é estabelecida
def on_connect(client, userdata,flags, rc, properties=None):
    print("Conectado ao MQTT Broker")
    print("Código de resultado de conexão: " + str(rc))

# callback para verificar se a publicação foi bem sucedida:
def on_publish(client, userdata, mid, properties=None):
    print("mid: " + str(mid))

# Printa em qual tópico se inscreveu
def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))

# Printa uma mensagem, útil para checar se a conexão foi bem sucedida
def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    print("Mensagem recebida: " + payload)
  #


start_time = datetime(2023, 6, 19, 11, 45, 0)
end_time = datetime(2023, 6, 19, 11, 49, 0)


catch_data(start_time, end_time, collection_today) 