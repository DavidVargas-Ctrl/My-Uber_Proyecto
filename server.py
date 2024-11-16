"""
Uso:
    python server.py --cuadricula_N <N> --cuadricula_M <M> --zmq_port <zmq_port>

Ejemplo:
    python server.py --cuadricula_N 50 --cuadricula_M 50 --zmq_puerto 5555
"""

import threading
import time
import random
import argparse
import sys
from collections import defaultdict
import paho.mqtt.client as mqtt
import zmq

def proceso_servidor(N, M, mqtt_broker_address, mqtt_broker_port, zmq_port):
    """
    Funcion principal para el proceso del servidor.

    Parametros:
        N (int): Tamaño N (filas) de la cuadrícula.
        M (int): Tamaño M (columnas) de la cuadrícula.
        mqtt_broker_address (str): Dirección del broker MQTT.
        mqtt_broker_port (int): Puerto del broker MQTT.
        zmq_port (int): Puerto para el servidor ZeroMQ.
    """

    pos_taxi = defaultdict(tuple)        # {taxi_id: (x, y)}
    pos_lock = threading.Lock()

    servicios_taxi = defaultdict(int)    # {taxi_id: numero_de_servicios}
    servicios_lock = threading.Lock()

    # Configuracion de ZeroMQ
    context = zmq.Context()
    socket_zmq = context.socket(zmq.REP)
    socket_zmq.bind(f"tcp://*:{zmq_port}")
    print(f"Servidor ZeroMQ escuchando en puerto {zmq_port}")

    def conexion(client, userdata, flags, rc, properties=None):
        if rc == 0:
            print("Servidor MQTT conectado al broker exitosamente.")
            # Suscribirse a todos los tópicos de posición de taxis y completados
            client.subscribe("taxis/+/posicion")
            client.subscribe("taxis/+/completado")
        else:
            print(f"Error al conectar al broker MQTT, codigo de retorno {rc}")
            sys.exit(1)

    def mensaje(client, userdata, msg):
        try:
            topic_parts = msg.topic.split('/')
            if len(topic_parts) < 3 or topic_parts[0] != 'taxis':
                print(f"Mensaje recibido en tópico desconocido: {msg.topic}")
                return

            taxi_id = int(topic_parts[1])

            if topic_parts[2] == 'posicion':
                # Manejar actualización de posición
                x_str, y_str = msg.payload.decode().split()
                x = int(x_str)
                y = int(y_str)

                with pos_lock:
                    # Verificar si el taxi está dentro de los límites
                    if 0 <= x <= N and 0 <= y <= M:
                        new_registration = taxi_id not in pos_taxi
                        pos_taxi[taxi_id] = (x, y)
                        if new_registration:
                            print(f"**Nuevo Taxi Registrado: ID {taxi_id} en posición ({x}, {y})**")
                        else:
                            print(f"Posición actual del Taxi {taxi_id}: ({x}, {y})")
                    else:
                        print(f"Taxi {taxi_id} intentó moverse fuera de los límites. Posición actual: ({x}, {y})")
                        # Ajustar la posición del taxi dentro de los límites
                        x = max(0, min(x, N))
                        y = max(0, min(y, M))
                        pos_taxi[taxi_id] = (x, y)
                        print(f"Posición ajustada del Taxi {taxi_id}: ({x}, {y})")

            elif topic_parts[2] == 'completado':
                # Manejar completacion de servicio
                with servicios_lock:
                    if servicios_taxi[taxi_id] > 0:
                        servicios_taxi[taxi_id] -= 1
                        print(f"Taxi {taxi_id} ha completado un servicio. Servicios asignados: {servicios_taxi[taxi_id]}")
                    else:
                        print(f"Taxi {taxi_id} intenta completar un servicio pero no tiene servicios asignados.")

            else:
                print(f"Mensaje recibido en tópico desconocido: {msg.topic}")

        except Exception as e:
            print(f"Error al procesar mensaje: {e}")

    # Configuracion del cliente MQTT con protocolo actualizado
    client = mqtt.Client(client_id="Servidor", protocol=mqtt.MQTTv5)

    client.on_connect = conexion
    client.on_message = mensaje

    try:
        client.connect(mqtt_broker_address, mqtt_broker_port, 60)
    except Exception as e:
        print(f"No se pudo conectar al broker MQTT: {e}")
        sys.exit(1)

    client.loop_start()

    def manejar_solicitudes_zmq():
        """
        Manejar solicitudes de usuarios via ZeroMQ.
        """
        while True:
            try:
                mensaje = socket_zmq.recv_string()
                user_id, user_x, user_y = map(int, mensaje.split())
                print(f"Recibida solicitud de Taxi para Usuario {user_id} en posición ({user_x}, {user_y})")
                start_time = time.time()

                with pos_lock, servicios_lock:
                    # Filtrar taxis disponibles (servicios < 3)
                    taxis_disponibles = [taxi for taxi, servicios in servicios_taxi.items() if servicios < 3]
                    # Incluir taxis que no tienen servicios asignados aún
                    taxis_disponibles += [taxi for taxi in pos_taxi.keys() if taxi not in servicios_taxi or servicios_taxi[taxi] < 3]
                    # Remover duplicados
                    taxis_disponibles = list(set(taxis_disponibles))

                    if not taxis_disponibles:
                        respuesta = "NO Taxi disponibles en este momento."
                        socket_zmq.send_string(respuesta)
                        print(f"Respuesta a Usuario {user_id}: {respuesta}")
                        continue

                    # Seleccionar el taxi más cercano
                    taxi_seleccionado = None
                    distancia_min = float('inf')
                    for taxi in taxis_disponibles:
                        taxi_pos = pos_taxi.get(taxi, None)
                        if taxi_pos:
                            distancia = abs(taxi_pos[0] - user_x) + abs(taxi_pos[1] - user_y)
                            if distancia < distancia_min:
                                distancia_min = distancia
                                taxi_seleccionado = taxi

                    if taxi_seleccionado is not None:
                        servicios_taxi[taxi_seleccionado] += 1
                        respuesta = f"OK {taxi_seleccionado}"
                        socket_zmq.send_string(respuesta)
                        end_time = time.time()
                        tiempo_respuesta = end_time - start_time
                        tiempo_programa = tiempo_respuesta  # 1 segundo = 1 minuto

                        print(f"Asignado Taxi {taxi_seleccionado} al Usuario {user_id} en {tiempo_programa:.2f} minutos.")

                        # Notificar al taxi del servicio asignado
                        topic_servicio = f"taxis/{taxi_seleccionado}/servicio"
                        mensaje_servicio = f"Usuario {user_id} en ({user_x}, {user_y})"
                        client.publish(topic_servicio, mensaje_servicio)
                        print(f"Notificado al Taxi {taxi_seleccionado} sobre el servicio asignado al Usuario {user_id}.")

                    else:
                        respuesta = "NO Taxi disponibles."
                        socket_zmq.send_string(respuesta)
                        print(f"Respuesta a Usuario {user_id}: {respuesta}")

            except Exception as e:
                print(f"Error al manejar solicitud ZeroMQ: {e}")

    # Iniciar el hilo para manejar solicitudes de usuarios
    thread_zmq = threading.Thread(target=manejar_solicitudes_zmq, daemon=True)
    thread_zmq.start()

    # Mantener el hilo principal activo
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Servidor detenido.")
        client.loop_stop()
        client.disconnect()
        socket_zmq.close()
        context.term()
        sys.exit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Proceso del Servidor")
    parser.add_argument('--cuadricula_N', type=int, required=True, help='Tamaño N de la cuadricula')
    parser.add_argument('--cuadricula_M', type=int, required=True, help='Tamaño M de la cuadricula')
    parser.add_argument('--zmq_puerto', type=int, default=5555, help='Puerto para ZeroMQ')
    parser.add_argument('--mqtt_broker_address', type=str, default='test.mosquitto.org', help='Dirección del broker MQTT')
    parser.add_argument('--mqtt_broker_port', type=int, default=1883, help='Puerto del broker MQTT')
    args = parser.parse_args()

    proceso_servidor(
        N=args.cuadricula_N,
        M=args.cuadricula_M,
        mqtt_broker_address=args.mqtt_broker_address,
        mqtt_broker_port=args.mqtt_broker_port,
        zmq_port=args.zmq_puerto
    )
