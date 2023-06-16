import paho.mqtt.client as paho
from paho import mqtt
import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime
import infos_sensiveis

# Configurações do MQTT Broker
broker_address = infos_sensiveis.broker_address
port = infos_sensiveis.port_broker
topic = infos_sensiveis.topic_mqtt

# Configurações do dispositivo
app_id = topic.split("/")[0]
service_id = topic.split("/")[1]

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

#Define as regras de validação:
validation_rules = {
  '$jsonSchema': {
    'bsonType': 'object',
    'required': [
      'application',
      'schema_version',
      'send'
    ],
    'properties': {
      'application': {
        'bsonType': 'string',
      },
      'schema_version': {
        'bsonType': 'int'
      },
      'dynamicField': {
        'bsonType': [
          'object',
          'null'
        ],
        'properties': {
          'current': {
            'bsonType': 'double'
          },
          'frequency': {
            'bsonType': 'double'
          },
          'timestamp': {
            'bsonType': 'date'
          }
        }
      }
    },
    'additionalProperties': True
  }
}

#Definição do db e collection
db= client_db['BancoTeste2']
today = datetime.utcnow().date()
collection_today = f"medidas_{today}"

# Checa se já há uma collection do dia atual, caso não cria uma considerando regras de validação
collection_names = db.list_collection_names()

if collection_today not in collection_names:
    collection = db.create_collection(collection_today, validator = validation_rules)
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

    # identifica o application 
    data["application"] = app_id

    # Identifica a versão do Schema:
    data["schema_version"] = 1

    # identifica o Service:
    data[service_id] = {}

    # adiciona a mensagem recebida em data[service]
    for pair in pairs:
        key, value = pair.split('=')
        data[service_id][key.strip()] = float(value)
    
    #Adiciona a data atual da mensagem recebida:
    timestamp = datetime.utcnow()

    data[service_id]["timestamp"] = timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    
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

# Encerra o loop e desconecta do MQTT Broker
#client.loop_stop()
#client.disconnect()

