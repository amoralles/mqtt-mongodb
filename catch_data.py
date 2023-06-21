from datetime import datetime, timedelta
from bson import json_util
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import infos_sensiveis
import pytz
import paho.mqtt.client as paho
from paho import mqtt
import json

# Converte a Data e Hora do horário BR para o horário Padrão UTC
def br_to_utc(dt):
    br_timezone = pytz.timezone('America/Sao_Paulo')
    utc_timezone = pytz.utc
    dt_br = br_timezone.localize(dt)
    dt_utc = dt_br.astimezone(utc_timezone)
    return dt_utc

# Lê arquivos json
def read_json_file(filename):
    global content
    with open(filename, 'r') as file:
        content = file.read()
    return content

# Função para extrair o intervalo de tempo da msg
def time_interval(msg):
    partes = msg.split()
    global start_time, end_time, interval
    if len(partes) == 2:
        quantidade = int(partes[0])
        unidade = partes[1].lower()
        if unidade == "min" or unidade == "minutos" or unidade == "minuto":
            interval = timedelta(minutes=quantidade)
        elif unidade == "hora" or unidade == "horas":
            interval =  timedelta(hours=quantidade)
        elif unidade == "dia" or unidade == "dias":
            interval = timedelta(days=quantidade)  
    end_time = datetime.now()
    start_time = end_time - interval

    return interval, start_time, end_time

# Consulta a base de dados em um dado intervalo:
def catch_files(start_time, end_time):
    # Muda o timezone de BR para UTC
    start_time_utc = br_to_utc(start_time)
    end_time_utc = br_to_utc(end_time)

    # Seleciona o dia de início e fim do intervalo.
    start_date = start_time_utc.date()
    end_date = end_time_utc.date()

    #Converte as datas para strings no formato ISO8601
    start_time_iso = start_time_utc.isoformat() + "Z"
    end_time_iso = end_time_utc.isoformat() + "Z"  
    
    
    #conecta com a collection e recupera os dados:
    if start_date == end_date:
        collection_today = f"medidas_{start_date}"
        collection = db[collection_today]

        # Cria um filtro para os documentos dentro do intervalo de tempo desejado
        filter = {"send.timestamp": {"$gte": start_time_iso, "$lte": end_time_iso}}
    
        documents = list(collection.find(filter))
        print(len(documents)," arquivos encontrados.")
    else:
        collection_start = f"medidas_{start_date}"
        collection1 = db[collection_start]

        collection_end = f"medidas{end_date}"
        collection2 = db[collection_end]

        #Cria os filtos utilizados em cada cosulta:
        filter1 = {"send.timestamp": {"$gte": start_time_iso}}
        filter2 = {"send.timestamp": {"$lte": end_time_iso}}

        #Faz as consultas:
        documents1 = list(collection1.find(filter1))
        documents2 = list(collection2.find(filter2))

        #Concatena todos os arquivos encotrados:
        documents = documents1 + documents2
        print(len(documents)," arquivos encontrados.")

    # Salva os documentos em um arquivo JSON
    global filename
    filename = "files_{}_{}.json".format(start_time.strftime("%Y-%m-%dT%H-%M-%SZ"), end_time.strftime("%Y-%m-%dT%H-%M-%SZ"))
    with open(filename, "w") as file:
        for document in documents:
            json_data = json_util.dumps(document)
            file.write(json_data)
            file.write("\n")
    
    print("Arquivo salvo com sucesso.")

    return filename


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

# Configuração do MQTT Broker:
broker_address = infos_sensiveis.broker_address
port = infos_sensiveis.port_broker
topic_request = "+/request"

# Função de callback chamada quando a conexão ao broker é estabelecida
def on_connect(client, userdata,flags, rc, properties=None):
    print("Conectado ao MQTT Broker")
    print("Código de resultado de conexão: " + str(rc))

# callback para verificar se a publicação foi bem sucedida:
def on_publish(client, userdata, mid, properties=None):
    print("Publicado em " + topic_response + ": " + filename)

# Printa em qual tópico se inscreveu
def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))

# Lida com a mensagem recebida
def on_message(client, userdata, msg):
    # Obtém o tópico que enviou a resposta em "+/request"
    global first_level
    topic_parts = msg.topic.split('/')
    first_level = topic_parts[0]

    global payload, date_day
    payload = msg.payload.decode()
    print("Request enviada por: ", first_level, "\nMensagem: ", payload)
 
    #define o intervalo solicitado:
    interval, datetime_inicial, datetime_final = time_interval(payload)

    # Consulta o DB e filtra os arquivos solicitados:
    filename = catch_files(datetime_inicial, datetime_final)

    global topic_response
    topic_response = "{}/response".format(first_level)

    read_json_file(filename)

    client.publish(topic_response, content)      

# Cria um cliente MQTT
client = paho.Client(client_id="", userdata=None, protocol=paho.MQTTv5)
client.on_connect = on_connect

#enable TLS
client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)

# Define user e senha
user = infos_sensiveis.user_mqtt
password = infos_sensiveis.password_mqtt
client.username_pw_set(infos_sensiveis.user_mqtt, infos_sensiveis.password_mqtt)

# Conecta ao MQTT Broker
client.connect(broker_address, port=port)

# Definição do DB 
db= client_db['BancoTeste2']

# Define as funções de callback
client.on_subscribe = on_subscribe
client.on_message = on_message
client.on_publish = on_publish

# Se inscreve nos tópicos
client.subscribe(topic_request, qos = 1)

# Loop principal
client.loop_forever()
