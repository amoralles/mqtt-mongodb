import paho.mqtt.client as paho
from paho import mqtt
import random
import time

# Configurações do MQTT Broker
broker_address = "c70e89f631f4472194a8a7cb223c5841.s2.eu.hivemq.cloud"
port = 8883
topic = "esp100001/send"

# Configurações do dispositivo
device_id = "disp1"

# Função de callback chamada quando a conexão ao broker é estabelecida
def on_connect(client, userdata,flags, rc, properties=None):
    print("Conectado ao MQTT Broker.")
    print("Código de resultado de conexão: " + str(rc))

# callback para verificar se a publicação foi bem sucedida:
def on_publish(client, userdata, mid, properties=None):
    print("mid: " + str(mid))

# Função para publicar a temperatura no tópico MQTT
def publish_temperature(client):
    temperature = round(random.uniform(0, 2),2)  # Simula uma temperatura entre 20°C e 30°C
    payload = "current=" + str(temperature) +",frequency=4.06"
    client.publish(topic, payload)
    print("Publicado: " + payload)

# Printa em qual tópico se inscreveu
def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))

# Printa uma mensagem, útil para checar se a conexão foi bem sucedida
def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))

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
client.subscribe("temperatura/teste", qos = 1)

# Loop principal
client.loop_start()

try:
    while True:
        publish_temperature(client)
        time.sleep(15)  # Publica a temperatura a cada 5 segundos

except KeyboardInterrupt:
    pass

#client.loop_forever()

# Encerra o loop e desconecta do MQTT Broker
#client.loop_stop()
#client.disconnect()

