"""
Uso:
    python server.py --cuadricula_N <N> --cuadricula_M <M>

Ejemplo:
    python server.py --cuadricula_N 50 --cuadricula_M 50
"""

import threading
import time
import argparse
import sys
from collections import defaultdict
import paho.mqtt.client as mqtt
import zmq
import signal

# Configuración de los brokers MQTT
PRIMARY_BROKER_ADDRESS = "test.mosquitto.org"
PRIMARY_BROKER_PORT = 1883
SECONDARY_BROKER_ADDRESS = "broker.hivemq.com"
SECONDARY_BROKER_PORT = 1883

def conectar_broker(client, primary=True):
    """
    Conecta al broker primario o secundario.

    Parámetros:
        client (mqtt.Client): Cliente MQTT.
        primary (bool): Si True, intenta conectarse al broker primario; si False, al secundario.
    """
    broker_address = PRIMARY_BROKER_ADDRESS if primary else SECONDARY_BROKER_ADDRESS
    broker_port = PRIMARY_BROKER_PORT if primary else SECONDARY_BROKER_PORT

    try:
        client.connect(broker_address, broker_port, 60)
        print(f"Conectado al {'primario' if primary else 'secundario'} broker MQTT: {broker_address}:{broker_port}")
    except Exception as e:
        if primary:
            print(f"Error al conectar al broker primario: {e}. Intentando conectar al secundario...")
            conectar_broker(client, primary=False)
        else:
            print(f"Error al conectar al broker secundario: {e}. Finalizando aplicación.")
            sys.exit(1)

def proceso_servidor(N, M, mqtt_broker_address, mqtt_broker_port, zmq_port):
    """
    Función principal para el proceso del servidor.

    Parámetros:
        N (int): Tamaño N (filas) de la cuadrícula.
        M (int): Tamaño M (columnas) de la cuadrícula.
        mqtt_broker_address (str): Dirección del broker MQTT.
        mqtt_broker_port (int): Puerto del broker MQTT.
        zmq_port (int): Puerto para el servidor ZeroMQ.
    """

    pos_taxi = defaultdict(tuple)        # {taxi_id: (x, y)}
    pos_lock = threading.Lock()

    disponibles_taxi = defaultdict(bool) # {taxi_id: True/False}
    disponibles_lock = threading.Lock()

    servicios_completados = defaultdict(int)  # {taxi_id: servicios_completados}
    servicios_lock = threading.Lock()

    taxis_activos = set()  # IDs de taxis que han alcanzado su límite de servicios
    taxis_activos_lock = threading.Lock()

    registrados = set()  # IDs de taxis ya registrados
    registrados_lock = threading.Lock()

    # Configuración de ZeroMQ
    context = zmq.Context()
    socket_zmq = context.socket(zmq.REP)
    socket_zmq.bind(f"tcp://*:{zmq_port}")
    print(f"Servidor ZeroMQ escuchando en puerto {zmq_port}")

    # Evento para detener los hilos correctamente
    stop_event = threading.Event()

    def conexion(client, userdata, flags, rc, properties=None):
        if rc == 0:
            print("Servidor MQTT conectado al broker exitosamente.")
            # Suscribirse a todos los tópicos de posición de taxis y completados
            client.subscribe("taxis/+/posicion")
            client.subscribe("taxis/+/completado")
            client.subscribe("taxis/+/fin_jornada")
        else:
            print(f"Error al conectar al broker MQTT, código de retorno {rc}")
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

                with pos_lock, disponibles_lock, registrados_lock:
                    # Verificar si el taxi está dentro de los límites
                    if 0 <= x <= N and 0 <= y <= M:
                        new_registration = taxi_id not in pos_taxi
                        if new_registration:
                            with taxis_activos_lock:
                                if taxi_id in taxis_activos:
                                    # El taxi ha alcanzado su límite de servicios
                                    respuesta = "ID Taxi inactivo. No puede registrarse."
                                    client.publish(f"taxis/{taxi_id}/registro", respuesta)
                                    print(f"Taxi {taxi_id} intentó registrarse pero ya ha culminado su jornada.")
                                    return
                            pos_taxi[taxi_id] = (x, y)
                            disponibles_taxi[taxi_id] = True  # Marcar como disponible al registrarse
                            registrados.add(taxi_id)
                            print(f"**Nuevo Taxi Registrado: ID {taxi_id} en posición ({x}, {y})**")
                        else:
                            pos_taxi[taxi_id] = (x, y)
                            print(f"Posición actual del Taxi {taxi_id}: ({x}, {y})")
                    else:
                        print(f"Taxi {taxi_id} intentó moverse fuera de los límites. Posición actual: ({x}, {y})")
                        # Ajustar la posición del taxi dentro de los límites
                        x = max(0, min(x, N))
                        y = max(0, min(y, M))
                        pos_taxi[taxi_id] = (x, y)
                        print(f"Posición ajustada del Taxi {taxi_id}: ({x}, {y})")

            elif topic_parts[2] == 'completado':
                # Manejar completación de servicio
                with disponibles_lock, servicios_lock, taxis_activos_lock:
                    if disponibles_taxi[taxi_id] == False:
                        disponibles_taxi[taxi_id] = True
                        servicios_completados[taxi_id] += 1
                        print(f"Taxi {taxi_id} ha completado un servicio y ahora está disponible. Total servicios: {servicios_completados[taxi_id]}")

                        if servicios_completados[taxi_id] >= 3:
                            # Taxi ha alcanzado el límite de servicios
                            disponibles_taxi[taxi_id] = False  # No disponible para más servicios
                            taxis_activos.add(taxi_id)
                            print(f"Taxi {taxi_id} ha alcanzado el límite de servicios. No podrá aceptar más servicios.")
                            # Notificar al taxi que su jornada ha terminado
                            client.publish(f"taxis/{taxi_id}/fin_jornada", "Jornada laboral culminada. No puede aceptar más servicios.")
                    else:
                        print(f"Taxi {taxi_id} intenta completar un servicio pero ya está disponible.")
            elif topic_parts[2] == 'fin_jornada':
                # Manejar notificación de fin de jornada del taxi
                with disponibles_lock, taxis_activos_lock:
                    disponibles_taxi[taxi_id] = False
                    taxis_activos.add(taxi_id)
                    print(f"Taxi {taxi_id} ha notificado el fin de su jornada laboral.")
            else:
                print(f"Mensaje recibido en tópico desconocido: {msg.topic}")

        except Exception as e:
            print(f"Error al procesar mensaje: {e}")

    # Configuración del cliente MQTT con protocolo actualizado
    client = mqtt.Client(client_id="Servidor", protocol=mqtt.MQTTv5)

    client.on_connect = conexion
    client.on_message = mensaje

    # Conexión al broker MQTT con respaldo
    conectar_broker(client)

    client.loop_start()

    def manejar_solicitudes_zmq():
        """
        Manejar solicitudes de usuarios via ZeroMQ.
        """
        while not stop_event.is_set():
            try:
                # Usar poller para permitir la salida del loop
                poller = zmq.Poller()
                poller.register(socket_zmq, zmq.POLLIN)
                socks = dict(poller.poll(1000))  # Timeout de 1 segundo

                if socket_zmq in socks and socks[socket_zmq] == zmq.POLLIN:
                    mensaje = socket_zmq.recv_string()
                    user_id, user_x, user_y = map(int, mensaje.split(','))
                    print(f"Recibida solicitud de Taxi para Usuario {user_id} en posición ({user_x}, {user_y})")
                    start_time = time.time()

                    with disponibles_lock:
                        # Filtrar taxis disponibles dentro de la cuadrícula y que no han terminado su jornada
                        taxis_disponibles = [taxi for taxi, disponible in disponibles_taxi.items() if disponible and taxi not in taxis_activos]
                        if not taxis_disponibles:
                            # No hay taxis disponibles actualmente, informar al usuario que debe esperar
                            respuesta = "NO Taxi disponibles en este momento."
                            socket_zmq.send_string(respuesta)
                            print(f"Respuesta a Usuario {user_id}: {respuesta}")
                            continue

                        # Seleccionar el taxi más cercano usando la distancia de Manhattan
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
                            # Asignar el taxi seleccionado
                            disponibles_taxi[taxi_seleccionado] = False  # Marcar como no disponible
                            respuesta = f"OK {taxi_seleccionado}"
                            socket_zmq.send_string(respuesta)
                            end_time = time.time()
                            tiempo_respuesta = end_time - start_time
                            tiempo_programa = 30.0  # Fijar el tiempo de servicio a 30 segundos

                            print(f"Asignado Taxi {taxi_seleccionado} al Usuario {user_id}")

                            # Notificar al taxi del servicio asignado
                            topic_servicio = f"taxis/{taxi_seleccionado}/servicio"
                            mensaje_servicio = f"Usuario {user_id}, {user_x} {user_y}"
                            client.publish(topic_servicio, mensaje_servicio)
                            print(f"Notificado al Taxi {taxi_seleccionado} sobre el servicio asignado al Usuario {user_id}.")
                        else:
                            # No se encontró ningún taxi disponible después de la espera
                            respuesta = "NO Taxi disponibles en este momento."
                            socket_zmq.send_string(respuesta)
                            print(f"Respuesta a Usuario {user_id}: {respuesta}")

            except zmq.ContextTerminated:
                # Contexto terminado, salir del loop
                break
            except Exception as e:
                print(f"Error al manejar solicitud ZeroMQ: {e}")

    # Iniciar el hilo para manejar solicitudes de usuarios
    thread_zmq = threading.Thread(target=manejar_solicitudes_zmq)
    thread_zmq.start()

    def shutdown(signum, frame):
        """
        Función para manejar el cierre del servidor de manera ordenada.
        """
        print("Servidor detenido.")
        stop_event.set()
        thread_zmq.join(timeout=2)
        client.loop_stop()
        client.disconnect()
        socket_zmq.close()
        context.term()
        sys.exit(0)

    # Manejar señales de interrupción para cerrar el servidor correctamente
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Mantener el hilo principal activo
    while not stop_event.is_set():
        time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Proceso del Servidor")
    parser.add_argument('--cuadricula_N', type=int, required=True, help='Tamaño N de la cuadrícula')
    parser.add_argument('--cuadricula_M', type=int, required=True, help='Tamaño M de la cuadrícula')
    args = parser.parse_args()

    proceso_servidor(
        N=args.cuadricula_N,
        M=args.cuadricula_M,
        mqtt_broker_address='test.mosquitto.org',
        mqtt_broker_port=1883,
        zmq_port=5555
    )