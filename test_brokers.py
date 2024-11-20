import paho.mqtt.client as mqtt

broker_addresses = ['invalid.broker.address', 'test.mosquitto.org', 'broker.hivemq.com']
broker_port = 1883

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Conectado exitosamente al broker {userdata['broker_address']}")
    else:
        print(f"Error al conectar al broker {userdata['broker_address']}, código de retorno {rc}")

def on_disconnect(client, userdata, rc):
    print(f"Desconectado del broker {userdata['broker_address']}")

def test_broker(broker_address, broker_port):
    client = mqtt.Client(client_id="TestClient")
    client.user_data_set({'broker_address': broker_address})
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    try:
        client.connect(broker_address, broker_port, 60)
        client.loop_start()
        client.loop_stop()
        client.disconnect()
    except Exception as e:
        print(f"No se pudo conectar al broker {broker_address}: {e}")

for broker_address in broker_addresses:
    test_broker(broker_address, broker_port)