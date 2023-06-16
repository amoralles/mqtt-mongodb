import paho.mqtt.client as paho
from paho import mqtt
import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime

# Configurações do MQTT Broker
broker_address = "c70e89f631f4472194a8a7cb223c5841.s2.eu.hivemq.cloud"
port = 8883
topic = "esp100001/send"

# Configurações do dispositivo
app_id = topic.split("/")[0]
service_id = topic.split("/")[1]

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


#Definição do db e collection
db= client_db['BancoTeste1']
today = datetime.utcnow().date()
collection_today = f"medidas_{today}"

# Checa se já há uma collection do dia atual, caso não cria uma considerando regras de validação
collection_names = db.list_collection_names()

if collection_today not in collection_names:
    collection = db.create_collection(collection_today, 
                                      {
                                          'timeseries': {
                                              'timeField': "timestamp",
                                              'metaField': "metadata",
                                              'granularity': "seconds"
                                          }
                                      })
else:
    collection = db[collection_today] 

# Função de callback chamada quando a conexão ao broker é estabelecida
def on_connect(client, userdata,flags, rc, properties=None):
    print("Conectado ao MQTT Broker sob o tópico: " + topic)
    print("Código de resultado de conexão: " + str(rc))

# callback para verificar se a publicação foi bem sucedida:
def on_publish(client, userdata, mid, properties=None):
    print("mid: " + str(mid))

# Função para salvar a temperatura no tópico MQTT
def salvar_mensagem(payload, app_id, service_id):
    data = {}
    pairs = payload.split(',')

    # identifica o application e service como metadados 
    data["metadata"] = {'application': app_id, 'service': service_id}

    # adiciona informação de tempo
    timestamp = datetime.utcnow()
    data['timestamp'] = timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    # adiciona a mensagem recebida em data[service]
    for pair in pairs:
        key, value = pair.split('=')
        data[key.strip()] = float(value)
    
    #converte em formato json:
    json_data = json.dumps(data)

    #Salva o objeto JSON em um arquivo individual:
    filename = "{}_{}.json".format(app_id, timestamp.strftime("%Y-%m-%dT%H-%M-%SZ"))
    with open(filename,"w") as file:
        file.write(json_data)
    
    # Salva no db
    with open(filename) as file:
        output = json.load(file)
        collection.insert_one(output)

# Printa em qual tópico se inscreveu
def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))

# Printa uma mensagem, útil para checar se a conexão foi bem sucedida
def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    print("Mensagem recebida: " + payload)
    salvar_mensagem(payload, app_id, service_id)


# Cria um cliente MQTT
client = paho.Client(client_id="", userdata=None, protocol=paho.MQTTv5)
client.on_connect = on_connect

#enable TLS
client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)

# Define user e senha
client.username_pw_set("temper", "Teste001")

# Conecta ao MQTT Broker
client.connect(broker_address, port=port)

# Define as funções de callback
client.on_subscribe = on_subscribe
client.on_message = on_message
client.on_publish = on_publish

# Se inscreve nos tópicos
client.subscribe(topic, qos = 1)


client.loop_forever()