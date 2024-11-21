import time
import argparse
import paho.mqtt.client as mqtt

def health_check_mqtt(mqtt_host, mqtt_port, check_interval):
    """
    Health Check para monitorear un broker MQTT enviando mensajes periódicos.
    Si el broker está desactivado, intenta reconectar y seguir publicando.
    """
    mqtt_client = mqtt.Client()
    connected = False

    def on_connect(client, userdata, flags, rc):
        nonlocal connected
        if rc == 0:
            print(f"Conexión establecida con el broker MQTT en {mqtt_host}:{mqtt_port}.")
            connected = True
        else:
            print(f"Error al conectar con el broker MQTT. Código: {rc}")
            connected = False

    def on_disconnect(client, userdata, rc):
        nonlocal connected
        print("Conexión con el broker MQTT perdida.")
        connected = False

    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect

    while True:
        try:
            if not connected:
                print("Intentando conectar al broker MQTT...")
                mqtt_client.connect(mqtt_host, mqtt_port, 60)
                mqtt_client.loop_start()

            if connected:
                try:
                    mqtt_client.publish("health/check", "PING")
                    print(f"Mensaje 'PING' enviado al broker MQTT ({mqtt_host}:{mqtt_port}). - {time.ctime()}")
                except Exception as e:
                    print(f"Error al intentar publicar el mensaje: {e}")
                    connected = False
                    mqtt_client.loop_stop()  # Detener el bucle si hay problemas al publicar
            time.sleep(check_interval)
        except Exception as e:
            time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Health Check para monitorear un broker MQTT enviando mensajes.")
    parser.add_argument('--mqtt_host', type=str, required=True, help="Dirección del broker MQTT.")
    parser.add_argument('--mqtt_port', type=int, required=True, help="Puerto del broker MQTT.")
    parser.add_argument('--check_interval', type=int, default=5, help="Intervalo en segundos entre cada verificación.")
    args = parser.parse_args()

    health_check_mqtt(args.mqtt_host, args.mqtt_port, args.check_interval)
