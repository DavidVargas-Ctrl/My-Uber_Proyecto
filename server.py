"""
Uso:
    python server.py --cuadricula_N <N> --cuadricula_M <M>

Ejemplo:
    python server.py --cuadricula_N 50 --cuadricula_M 50
    python server.py --cuadricula_N 50 --cuadricula_M 50 --port 1884
    python server.py --cuadricula_N 50 --cuadricula_M 50 --brokers 10.43.101.195:1883


"""

import threading
import time
import random
import argparse
import sys
from collections import defaultdict
import paho.mqtt.client as mqtt

def proceso_servidor(N, M, brokers):
    """
    Función principal para el proceso del servidor.

    Parámetros:
        N (int): Tamaño N (filas) de la cuadrícula.
        M (int): Tamaño M (columnas) de la cuadrícula.
        broker_address (str): Dirección del broker MQTT.
        broker_port (int): Puerto del broker MQTT.
    """

    pos_taxi = defaultdict(tuple)    # {taxi_id: (x, y)}
    pos_lock = threading.Lock()

    clientes = []
    for idBroker, (broker_direccion, broker_puerto) in enumerate(brokers): # Enumera opara asignar un ID distintitvo, a cada broker
        client = mqtt.Client(client_id=f"Servidor_{idBroker}") # Aquí se crea un nuevo cliente MQTT para cada broker
        client.broker_address = broker_direccion
        client.broker_port = broker_puerto
        client.topic_position = "taxis/+/posicion" # Subscripcion del cliente

    def conexion(client, info, banderas, rc):
        if rc == 0:
            print("Servidor conectado al broker MQTT exitosamente.")
            # Suscribirse a todos los tópicos de posición de taxis
            client.subscribe("taxis/+/posicion")#Categoria + Topico

        else:
            print(f"Error al conectar al broker MQTT, código de retorno {rc}")
            sys.exit(1)

    def mensaje(cliente, info, msg):
        try:
            topic_parts = msg.topic.split('/')
            if len(topic_parts) != 3 or topic_parts[0] != 'taxis' or topic_parts[2] != 'posicion':
                print(f"Mensaje recibido en tópico desconocido: {msg.topic}")
                return

            taxi_id = int(topic_parts[1])
            x_str, y_str = msg.payload.decode().split()
            x = int(x_str)
            y = int(y_str)

            with pos_lock:
                # Verificar si el taxi está dentro de los límites
                if 0 <= x <= N and 0 <= y <= M:
                    pos_taxi[taxi_id] = (x, y)
                    print(f"Posición actual del Taxi {taxi_id}: ({x}, {y})")
                else:
                    print(f"Taxi {taxi_id} intentó moverse fuera de los límites. Posición actual: ({x}, {y})")
                    # Ajustar la posición del taxi dentro de los límites
                    x = max(0, min(x, N))
                    y = max(0, min(y, M))
                    pos_taxi[taxi_id] = (x, y)

        except Exception as e:
            print(f"Error al procesar mensaje: {e}")

    client.on_connect = conexion
    client.on_message = mensaje

    try:
        client.connect(broker_direccion, broker_puerto, 60)
    except Exception as e:
        print(f"No se pudo conectar al broker MQTT: {e}")
        sys.exit(1)

    clientes.append(client)

    # Iniciar todos los clientes MQTT
    for client in clientes:
        client.loop_start()

    def asignar_servicios():
        """
        Seleccionar aleatoriamente un taxi y envía una asignación de servicio.
        """
        while True:
            time.sleep(10)  # Asignar servicios cada 10 segundos
            with pos_lock:
                if pos_taxi:
                    taxi_id = random.choice(list(pos_taxi.keys()))
                    print(f"Asignando servicio al Taxi {taxi_id}")
                    # Aquí podrías publicar un mensaje de asignación de servicio si lo deseas
                else:
                    print("No hay taxis disponibles para asignar servicios")

    threading.Thread(target=asignar_servicios, daemon=True).start()

    # Mantener el hilo principal activo
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Servidor detenido.")
        client.loop_stop()
        client.disconnect()
        sys.exit()



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Proceso del Servidor")
    parser.add_argument('--cuadricula_N', type=int, required=True, help='Tamaño N de la cuadrícula')
    parser.add_argument('--cuadricula_M', type=int, required=True, help='Tamaño M de la cuadrícula')
    parser.add_argument('--brokers', type=str, required=True,
                        help='Lista de brokers en formato ip:puerto, ip_n:puerto_n separados por comas')
    args = parser.parse_args()

    brokers = []
    for broker in args.brokers.split(','):
        ip, port = broker.split(':')
        brokers.append((ip.strip(), int(port.strip())))

    proceso_servidor(N=args.cuadricula_N,
                     M=args.cuadricula_M,
                     brokers=brokers)