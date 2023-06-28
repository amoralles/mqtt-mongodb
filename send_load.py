from datetime import datetime, timedelta
from bson import json_util
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import infos_sensiveis
import pytz
import paho.mqtt.client as paho
from paho import mqtt
import json

# Configurações do MQTT Broker
broker_address = infos_sensiveis.broker_address
port = infos_sensiveis.port_broker
topic = "+/status"

# Confgurações do Banco de Dados:
uri = infos_sensiveis.uri_mdb

# Create a new client and connect to the server
client_db = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client_db.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

# Define o banco de Dados:
db= client_db['BancoTeste2']
collection = db['Users_info']

# Função que Solicita o load
def ask_for_load(first_level):
    #Arquivo load pré existe no banco de dados com infos do usuário
    # Seleciona o load para aquele app
    filter = {"user": first_level}
    file_load = collection.find_one(filter)

    if file_load:
        filename = "load.json"
        with open(filename, "w") as file:
            json_data = json_util.dumps(file_load)
            file.write(json_data)

        return filename
    else:
        print('Arquivo load não encontrado para a aplicação', first_level)
        return None

# Lê arquivos json
def read_json_file(filename):
    global content
    with open(filename, 'r') as file:
        content = file.read()
    return content

# Função de callback chamada quando a conexão ao broker é estabelecida
def on_connect(client, userdata,flags, rc, properties=None):
    print("Conectado ao MQTT Broker.")
    print("Código de resultado de conexão: " + str(rc))

# callback para verificar se a publicação foi bem sucedida:
def on_publish(client, userdata, mid, properties=None):
    print("Publicado em " + topic_response)

# Printa em qual tópico se inscreveu
def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))

# Lida com a mensagem recebida
def on_message(client, userdata, msg):
    # Obtém o tópico que enviou a resposta em "+/request"
    global first_level
    topic_parts = msg.topic.split('/')
    first_level = topic_parts[0]

    global payload
    payload = msg.payload.decode()
    topic_sub = "{}/status".format(first_level)
    print("Mensagem enviada por: ", first_level, "no tópico: ", topic_sub, "\nMensagem: ", payload)

    global topic_response
    topic_response = "{}/load".format(first_level)

    if str(payload) == str(0):
        #Recupera o arquivo load do banco de dados
        filename = ask_for_load(first_level)
        if filename:
            content = read_json_file(filename)
            client.publish(topic_response, content) 
        else:
            ('Arquivo load não encontrado para a aplicação ', first_level)

# Cria um cliente MQTT
client = paho.Client(client_id="", userdata=None, protocol=paho.MQTTv5)
client.on_connect = on_connect

#enable TLS
client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)

# Define user e senha
user = infos_sensiveis.user_mqtt
password = infos_sensiveis.password_mqtt
client.username_pw_set(user, password)

# Conecta ao MQTT Broker
client.connect(broker_address, port=port)

# Define as funções de callback
client.on_subscribe = on_subscribe
client.on_message = on_message
client.on_publish = on_publish

# Se inscreve nos tópicos
client.subscribe(topic, qos = 1)

# Loop principal
client.loop_forever()