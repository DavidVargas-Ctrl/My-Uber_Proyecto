"""
Uso:
    python server.py --cuadricula_N <N> --cuadricula_M <M> [--intervalo_guardado <segundos>]

Ejemplo:
    python server.py --cuadricula_N 50 --cuadricula_M 50 --intervalo_guardado 20
"""

import threading
import time
import argparse
import sys
from collections import defaultdict
import paho.mqtt.client as mqtt
import zmq
import signal
from datetime import datetime

def proceso_servidor(N, M, mqtt_broker_address, mqtt_broker_port, zmq_port, intervalo_guardado):
    """
    Función principal para el proceso del servidor.

    Parámetros:
        N (int): Tamaño N (filas) de la cuadrícula.
        M (int): Tamaño M (columnas) de la cuadrícula.
        mqtt_broker_address (str): Dirección del broker MQTT.
        mqtt_broker_port (int): Puerto del broker MQTT.
        zmq_port (int): Puerto para el servidor ZeroMQ.
        intervalo_guardado (int): Intervalo en segundos para guardar el estado.
    """

    # Estructuras de datos para almacenar información de los taxis
    taxis_info = defaultdict(lambda: {
        'initial_pos': None,
        'positions': [],
        'servicios_completados': 0,
        'servicios_asignados': [],  # Lista de tuplas: (Pos Taxi, Pos Usuario)
        'available': True  # Bandera de disponibilidad
    })
    taxis_info_lock = threading.Lock()

    # Contadores globales
    total_servicios = 0  # Servicios asignados
    servicios_negados = 0
    contadores_lock = threading.Lock()

    # Configuración de ZeroMQ
    context = zmq.Context()
    socket_zmq = context.socket(zmq.REP)
    socket_zmq.bind(f"tcp://*:{zmq_port}")
    print(f"Servidor ZeroMQ escuchando en puerto {zmq_port}")

    # Evento para detener los hilos correctamente
    stop_event = threading.Event()

    # Configuración del archivo de registro
    log_lock = threading.Lock()
    log_file = "Interaccion.txt"

    def log_event(message):
        """
        Función para imprimir mensajes en consola.

        Parámetros:
            message (str): Mensaje a imprimir.
        """
        with log_lock:
            print(message)

    def guardar_estado():
        """
        Función para guardar el estado actual del sistema en el archivo de registro.
        """
        with taxis_info_lock, contadores_lock, log_lock:
            with open(log_file, 'w') as f:
                # Registro de taxis
                f.write("Registro de taxis\n\n")
                for taxi_id, info in sorted(taxis_info.items()):
                    if info['initial_pos']:
                        f.write(f"Taxi {taxi_id}, {info['initial_pos'][0]} {info['initial_pos'][1]}\n")
                        for pos in info['positions'][1:]:
                            f.write(f"{pos[0]} {pos[1]}\n")
                        f.write(f"Servicios\n{info['servicios_completados']}\n")
                        f.write("Servicios asignados\n")
                        for servicio in info['servicios_asignados']:
                            pos_taxi, pos_usuario = servicio
                            f.write(f"{pos_taxi[0]} {pos_taxi[1]}, {pos_usuario[0]} {pos_usuario[1]}\n")
                        f.write(f"Disponible: {'Sí' if info['available'] else 'No'}\n\n")

                # Resumen de los servicios
                f.write("Resumen de los servicios\n\n")
                f.write("Servicios aceptados\n")
                f.write(f"{total_servicios}\n\n")
                f.write("Servicios negados\n")
                f.write(f"{servicios_negados}\n\n")

                # Servicios asignados
                f.write("Servicios asignados\n")
                for taxi_id, info in sorted(taxis_info.items()):
                    for servicio in info['servicios_asignados']:
                        pos_taxi, pos_usuario = servicio
                        f.write(f"{pos_taxi[0]} {pos_taxi[1]}, {pos_usuario[0]} {pos_usuario[1]}\n")
                f.write("\n")

        log_event("Estado actual del sistema guardado en Interaccion.txt")

    def conexion(client, userdata, flags, rc, properties=None):
        if rc == 0:
            log_event("Servidor MQTT conectado al broker exitosamente.")
            # Suscribirse a todos los tópicos de posición de taxis y completados
            client.subscribe("taxis/+/posicion")
            client.subscribe("taxis/+/completado")
            client.subscribe("taxis/+/fin_jornada")
        else:
            error_msg = f"Error al conectar al broker MQTT, código de retorno {rc}"
            log_event(error_msg)
            with contadores_lock:
                nonlocal servicios_negados
                servicios_negados += 1
            guardar_estado()
            sys.exit(1)

    def mensaje(client, userdata, msg):
        nonlocal total_servicios, servicios_negados  # Usar nonlocal
        try:
            topic_parts = msg.topic.split('/')
            if len(topic_parts) < 3 or topic_parts[0] != 'taxis':
                desconocido_msg = f"Mensaje recibido en tópico desconocido: {msg.topic}"
                log_event(desconocido_msg)
                return

            taxi_id = int(topic_parts[1])

            if topic_parts[2] == 'posicion':
                # Manejar actualización de posición
                payload = msg.payload.decode().strip().split()
                if len(payload) != 2:
                    log_event(f"Formato de posición incorrecto para Taxi {taxi_id}: {msg.payload.decode()}")
                    return
                x_str, y_str = payload
                try:
                    x = int(x_str)
                    y = int(y_str)
                except ValueError:
                    log_event(f"Valores de posición no válidos para Taxi {taxi_id}: {msg.payload.decode()}")
                    return

                with taxis_info_lock:
                    if taxis_info[taxi_id]['initial_pos'] is None:
                        # Registro inicial del taxi
                        if taxis_info[taxi_id]['servicios_completados'] >= 3:
                            # El taxi ha alcanzado su límite de servicios
                            respuesta = "ID Taxi inactivo. No puede registrarse."
                            client.publish(f"taxis/{taxi_id}/registro", respuesta)
                            intento_msg = f"Taxi {taxi_id} intento registrarse pero ya ha culminado su jornada."
                            log_event(intento_msg)
                            with contadores_lock:
                                servicios_negados += 1
                            guardar_estado()
                            return
                        taxis_info[taxi_id]['initial_pos'] = (x, y)
                        taxis_info[taxi_id]['positions'].append((x, y))
                        # Registrar en el archivo
                        registro_msg = f"**Nuevo Taxi Registrado: ID {taxi_id} en posición ({x}, {y})**"
                        log_event(registro_msg)
                    else:
                        # Actualización de posición
                        taxis_info[taxi_id]['positions'].append((x, y))
                        actualizacion_msg = f"Posición actual del Taxi {taxi_id}: ({x}, {y})"
                        log_event(actualizacion_msg)

            elif topic_parts[2] == 'completado':
                # Manejar completación de servicio
                with taxis_info_lock, contadores_lock:
                    if taxis_info[taxi_id]['servicios_completados'] < 3:
                        taxis_info[taxi_id]['servicios_completados'] += 1
                        completado_msg = f"Taxi {taxi_id} ha completado un servicio. Total servicios: {taxis_info[taxi_id]['servicios_completados']}"
                        log_event(completado_msg)

                        # Marcar al taxi como disponible nuevamente
                        taxis_info[taxi_id]['available'] = True

                        if taxis_info[taxi_id]['servicios_completados'] == 3:
                            limite_msg = f"Taxi {taxi_id} ha alcanzado el límite de servicios. No podrá aceptar más servicios."
                            log_event(limite_msg)
                            # Notificar al taxi que su jornada ha terminado
                            client.publish(f"taxis/{taxi_id}/fin_jornada", "Jornada laboral culminada. No puede aceptar más servicios.")
                    else:
                        disponible_msg = f"Taxi {taxi_id} intenta completar un servicio pero ya ha alcanzado su límite."
                        log_event(disponible_msg)
                        with contadores_lock:
                            servicios_negados += 1
                        guardar_estado()

            elif topic_parts[2] == 'fin_jornada':
                # Manejar notificación de fin de jornada del taxi
                with taxis_info_lock, contadores_lock:
                    taxis_info[taxi_id]['servicios_completados'] = 3  # Forzar a que no pueda aceptar más servicios
                    taxis_info[taxi_id]['available'] = False  # Marcar como no disponible
                    fin_jornada_msg = f"Taxi {taxi_id} ha notificado el fin de su jornada laboral."
                    log_event(fin_jornada_msg)
                guardar_estado()
            else:
                desconocido_msg = f"Mensaje recibido en tópico desconocido: {msg.topic}"
                log_event(desconocido_msg)
                return

        except Exception as e:
            error_msg = f"Error al procesar mensaje: {e}"
            log_event(error_msg)
            with contadores_lock:
                servicios_negados += 1
            guardar_estado()
            return

    # Configuración del cliente MQTT con protocolo actualizado
    client = mqtt.Client(client_id="Servidor", protocol=mqtt.MQTTv5)

    client.on_connect = conexion
    client.on_message = mensaje

    try:
        client.connect(mqtt_broker_address, mqtt_broker_port, 60)
    except Exception as e:
        error_msg = f"No se pudo conectar al broker MQTT: {e}"
        log_event(error_msg)
        with contadores_lock:
            servicios_negados += 1
        guardar_estado()
        sys.exit(1)

    client.loop_start()

    def manejar_solicitudes_zmq():
        nonlocal total_servicios, servicios_negados  # Usar nonlocal
        """
        Manejar solicitudes de usuarios via ZeroMQ con un timeout de 60 segundos.
        """
        while not stop_event.is_set():
            try:
                # Usar poller para permitir la salida del loop
                poller = zmq.Poller()
                poller.register(socket_zmq, zmq.POLLIN)
                socks = dict(poller.poll(1000))  # Timeout de 1 segundo

                if socket_zmq in socks and socks[socket_zmq] == zmq.POLLIN:
                    mensaje_recibido = socket_zmq.recv_string()
                    try:
                        user_id, user_x, user_y = map(int, mensaje_recibido.strip().split(','))
                    except ValueError:
                        log_event(f"Formato de solicitud incorrecto: {mensaje_recibido}")
                        socket_zmq.send_string("Formato de solicitud incorrecto.")
                        with contadores_lock:
                            servicios_negados += 1
                        guardar_estado()
                        continue

                    solicitud_msg = f"{user_id}\t({user_x},{user_y})"
                    log_event(f"Recibida solicitud de Taxi para Usuario {user_id} en posición ({user_x}, {user_y})")
                    start_time = time.time()

                    taxi_asignado = None
                    asignacion_exitosa = False

                    while time.time() - start_time < 60:
                        with taxis_info_lock:
                            # Filtrar taxis disponibles dentro de la cuadrícula, que no han alcanzado su límite de servicios y están disponibles
                            taxis_disponibles = [
                                taxi for taxi, info in taxis_info.items()
                                if info['servicios_completados'] < 3 and info['initial_pos'] is not None and info['available']
                            ]
                            if taxis_disponibles:
                                # Seleccionar el taxi más cercano usando la distancia de Manhattan
                                taxi_seleccionado = None
                                distancia_min = float('inf')
                                pos_usuario = (user_x, user_y)
                                for taxi in taxis_disponibles:
                                    pos_taxi = taxis_info[taxi]['positions'][-1]  # Última posición del taxi
                                    distancia = abs(pos_taxi[0] - user_x) + abs(pos_taxi[1] - user_y)
                                    if distancia < distancia_min:
                                        distancia_min = distancia
                                        taxi_seleccionado = taxi

                                if taxi_seleccionado is not None:
                                    # Asignar el taxi seleccionado
                                    respuesta = f"OK {taxi_seleccionado}"
                                    socket_zmq.send_string(respuesta)
                                    asignacion_exitosa = True
                                    taxi_asignado = taxi_seleccionado

                                    asignacion_msg = f"Asignado Taxi {taxi_seleccionado} al Usuario {user_id}: Desde ({taxis_info[taxi_asignado]['positions'][-1][0]}, {taxis_info[taxi_asignado]['positions'][-1][1]}) hacia ({user_x}, {user_y})"
                                    log_event(asignacion_msg)

                                    # Registrar la asignación
                                    taxis_info[taxi_seleccionado]['servicios_asignados'].append( ((taxis_info[taxi_seleccionado]['positions'][-1][0], taxis_info[taxi_seleccionado]['positions'][-1][1]), (user_x, user_y)) )

                                    # Incrementar contador de servicios exitosos
                                    with contadores_lock:
                                        total_servicios += 1

                                    # Marcar al taxi como no disponible
                                    taxis_info[taxi_seleccionado]['available'] = False

                                    # Notificar al taxi del servicio asignado
                                    topic_servicio = f"taxis/{taxi_seleccionado}/servicio"
                                    mensaje_servicio = f"Usuario {user_id}, {user_x} {user_y}"
                                    client.publish(topic_servicio, mensaje_servicio)
                                    log_event(f"Notificado al Taxi {taxi_seleccionado} sobre el servicio asignado al Usuario {user_id}.")

                                    break  # Salir del loop de espera

                        if asignacion_exitosa:
                            guardar_estado()
                            break

                        time.sleep(1)  # Esperar 1 segundo antes de volver a intentar

                    if not asignacion_exitosa:
                        # No se encontró ningún taxi disponible después de 60 segundos
                        respuesta = "NO Taxi disponibles en este momento."
                        socket_zmq.send_string(respuesta)
                        negado_msg = f"{user_id}\tTimeout: No se asignó un taxi en 60 segundos."
                        log_event(negado_msg)
                        with contadores_lock:
                            servicios_negados += 1
                        guardar_estado()

            except zmq.ContextTerminated:
                # Contexto terminado, salir del loop
                break
            except Exception as e:
                error_msg = f"Error al manejar solicitud ZeroMQ: {e}"
                log_event(error_msg)
                with contadores_lock:
                    servicios_negados += 1
                guardar_estado()

    # Iniciar el hilo para manejar solicitudes de usuarios
    thread_zmq = threading.Thread(target=manejar_solicitudes_zmq)
    thread_zmq.start()

    def guardar_estado_periodicamente():
        """
        Hilo que guarda el estado periódicamente según el intervalo especificado.
        """
        while not stop_event.is_set():
            guardar_estado()
            time.sleep(intervalo_guardado)

    # Iniciar el hilo para guardar el estado periódicamente
    hilo_guardado = threading.Thread(target=guardar_estado_periodicamente)
    hilo_guardado.daemon = True  # Hilo en segundo plano
    hilo_guardado.start()

    def shutdown(signum, frame):
        """
        Función para manejar el cierre del servidor de manera ordenada.
        """
        log_event("\nServidor detenido.")
        stop_event.set()
        thread_zmq.join(timeout=2)
        client.loop_stop()
        client.disconnect()
        socket_zmq.close()
        context.term()

        # Registrar resumen de servicios al cerrar
        guardar_estado()

        # Añadir mensaje de cierre con timestamp
        with log_lock:
            with open(log_file, 'a') as f:
                f.write(f"Servidor detenido\n")

        log_event("Resumen de servicios registrado en Interaccion.txt")
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
    parser.add_argument('--intervalo_guardado', type=int, default=60, help='Intervalo en segundos para guardar el estado (default: 60)')
    args = parser.parse_args()

    proceso_servidor(
        N=args.cuadricula_N,
        M=args.cuadricula_M,
        mqtt_broker_address='test.mosquitto.org',
        mqtt_broker_port=1883,
        zmq_port=5555,
        intervalo_guardado=args.intervalo_guardado
    )
