import zmq
import time
import argparse
import paho.mqtt.client as mqtt

def health_server(zmq_host, zmq_port, mqtt_host, mqtt_port, check_interval):
    """
    Health Server para monitorear un servidor ZeroMQ y un broker MQTT.
    Uso:
        python health_check_server.py --zmq_host <direccion_host> --zmq_port <puerto> --mqtt_host <mqtt_host> --mqtt_port <mqtt_port> --check_interval <N>
    Ejemplo:
        python health_check_server.py --zmq_host localhost --zmq_port 5555 --mqtt_host 10.43.101.111 --mqtt_port 1883 --check_interval 5
    """
    # Configuración ZeroMQ
    context = zmq.Context()
    zmq_socket = context.socket(zmq.REQ)
    zmq_socket.setsockopt(zmq.RECONNECT_IVL, 1000)  # Tiempo de reconexión inicial (ms)
    zmq_socket.setsockopt(zmq.RECONNECT_IVL_MAX, 5000)  # Máximo tiempo de reconexión (ms)
    zmq_socket.connect(f"tcp://{zmq_host}:{zmq_port}")
    print(f"Health Server conectado a ZeroMQ en tcp://{zmq_host}:{zmq_port}")

    # Configuración MQTT
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Conectado al broker MQTT correctamente.")

        else:
            print(f"Error al conectar al broker MQTT, código: {rc}")

    def on_message(client, userdata, msg):
        print(f"Mensaje recibido del broker MQTT - Tema: {msg.topic}, Mensaje: {msg.payload.decode()}")

    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    try:
        mqtt_client.connect(mqtt_host, mqtt_port, 60)
        mqtt_client.loop_start()  # Inicia el bucle para manejar mensajes en segundo plano
    except Exception as e:
        print(f"Error al conectar con el broker MQTT: {e}")
        return

    # Ciclo principal de verificación
    while True:
        try:
            # Verificación ZeroMQ
            zmq_socket.send_string("PING")
            if zmq_socket.poll(2000, zmq.POLLIN):
                respuesta = zmq_socket.recv_string()
                if respuesta == "PONG":
                    print(f"Servidor ZeroMQ está operativo ({time.ctime()}).")
            else:
                print("No se recibió respuesta del servidor ZeroMQ (timeout).")
            mensaje
            # Verificación MQTT
            mqtt_client.publish("health/check", "PING")
            print(f"Mensaje PING enviado al broker MQTT ({mqtt_host}:{mqtt_port}).")
        except zmq.ZMQError as e:
            print(f"Error al verificar el servidor ZeroMQ: {e}")
        except Exception as e:
            print(f"Excepción inesperada: {e}")
        finally:
            time.sleep(check_interval)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Health Server para monitorear ZeroMQ y un broker MQTT.")
    parser.add_argument('--zmq_host', type=str, required=True, help="Dirección del servidor ZeroMQ.")
    parser.add_argument('--zmq_port', type=int, required=True, help="Puerto del servidor ZeroMQ.")
    parser.add_argument('--mqtt_host', type=str, required=True, help="Dirección del broker MQTT.")
    parser.add_argument('--mqtt_port', type=int, required=True, help="Puerto del broker MQTT.")
    parser.add_argument('--check_interval', type=int, default=5, help="Intervalo en segundos entre cada verificación.")
    args = parser.parse_args()

    health_server(args.zmq_host, args.zmq_port, args.mqtt_host, args.mqtt_port, args.check_interval)
