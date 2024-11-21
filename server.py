"""
Uso:
    python server.py --cuadricula_N <N> --cuadricula_M <M> [--intervalo_guardado <segundos>]

Ejemplo:
    python3 server.py --cuadricula_N 50 --cuadricula_M 50 --intervalo_guardado 10
"""

import threading
import time
import argparse
import sys
from collections import defaultdict
import paho.mqtt.client as mqtt
import zmq
import signal

def proceso_servidor(N, M, zmq_port, intervalo_guardado):
    """
    Función principal para el proceso del servidor.

    Parámetros:
        N (int): Tamaño N (filas) de la cuadrícula.
        M (int): Tamaño M (columnas) de la cuadrícula.
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

    # Contadores globales encapsulados en un diccionario
    global_counters = {
        'total_servicios': 0,  # Servicios asignados
        'servicios_negados': 0
    }
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

        """
        with log_lock:
            print(message)

    def guardar_estado():
        """
        Función para guardar el estado actual del sistema en el archivo de registro.
        """
        with taxis_info_lock, contadores_lock, log_lock:
            try:
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
                    f.write(f"{global_counters['total_servicios']}\n\n")
                    f.write("Servicios negados\n")
                    f.write(f"{global_counters['servicios_negados']}\n\n")

                    # Servicios asignados
                    f.write("Servicios asignados\n")
                    for taxi_id, info in sorted(taxis_info.items()):
                        for servicio in info['servicios_asignados']:
                            pos_taxi, pos_usuario = servicio
                            f.write(f"{pos_taxi[0]} {pos_taxi[1]}, {pos_usuario[0]} {pos_usuario[1]}\n")
                    f.write("\n")
            except Exception as e:
                log_event(f"Error al guardar el estado: {e}")
                return

        #log_event("Estado actual del sistema guardado en Interaccion.txt")

    # Configuración del cliente MQTT con múltiples brokers
    class MQTTManager:
        def __init__(self, brokers, global_counters, contadores_lock, guardar_estado_func):
            """
            Inicializa el gestor de MQTT con una lista de brokers.

            Parámetros:
                brokers (list): Lista de diccionarios con 'address' y 'port' de los brokers MQTT.
                global_counters (dict): Diccionario con contadores globales.
                contadores_lock (threading.Lock): Lock para los contadores.
                guardar_estado_func (function): Referencia a la función para guardar el estado.
            """
            self.brokers = brokers  # Lista de brokers
            self.current_broker = 0  # Índice del broker actual
            self.client = None
            self.lock = threading.Lock()
            self.connected_event = threading.Event()
            self.global_counters = global_counters
            self.contadores_lock = contadores_lock
            self.guardar_estado = guardar_estado_func
            self.initialize_client()

        def initialize_client(self):
            """
            Inicializa y conecta al broker MQTT actual.
            """
            broker = self.brokers[self.current_broker]
            self.client = mqtt.Client(client_id="Servidor", protocol=mqtt.MQTTv5)
            self.client.on_connect = self.on_connect
            self.client.on_disconnect = self.on_disconnect
            self.client.on_message = self.on_message

            try:
                self.client.connect(broker['address'], broker['port'], 60)
                self.client.loop_start()
                log_event(f"Intentando conectar al broker MQTT: {broker['address']}:{broker['port']}")
            except Exception as e:
                log_event(f"No se pudo conectar al broker MQTT {broker['address']}:{broker['port']} - {e}")
                self.switch_broker()

        def on_connect(self, client, userdata, flags, rc, properties=None):
            """
            Callback cuando el cliente se conecta al broker MQTT.

            Parámetros:
                client: Instancia del cliente.
                userdata: Datos del usuario.
                flags: Flags de conexión.
                rc: Código de retorno.
                properties: Propiedades (para MQTTv5).
            """
            if rc == 0:
                log_event(f"Conectado al broker MQTT {self.brokers[self.current_broker]['address']}:{self.brokers[self.current_broker]['port']}")
                # Suscribirse a todos los tópicos de posición de taxis y completados
                self.client.subscribe("taxis/+/posicion")
                self.client.subscribe("taxis/+/completado")
                self.client.subscribe("taxis/+/fin_jornada")
                self.connected_event.set()
            else:
                error_msg = f"Error al conectar al broker MQTT {self.brokers[self.current_broker]['address']}:{self.brokers[self.current_broker]['port']}, código de retorno {rc}"
                log_event(error_msg)
                with self.contadores_lock:
                    self.global_counters['servicios_negados'] += 1
                self.guardar_estado()
                self.switch_broker()

        def on_disconnect(self, client, userdata, rc, properties=None):
            """
            Callback cuando el cliente se desconecta del broker MQTT.

            Parámetros:
                client: Instancia del cliente.
                userdata: Datos del usuario.
                rc: Código de retorno.
            """
            log_event(f"Desconectado del broker MQTT {self.brokers[self.current_broker]['address']}:{self.brokers[self.current_broker]['port']} con código {rc}")
            self.connected_event.clear()
            if rc != 0:
                # Desconexión inesperada, intentar reconectar
                self.switch_broker()

        def on_message(self, client, userdata, msg):
            """
            Callback cuando se recibe un mensaje en un tópico suscrito.

            Parámetros:
                client: Instancia del cliente.
                userdata: Datos del usuario.
                msg: Mensaje recibido.
            """
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
                                self.client.publish(f"taxis/{taxi_id}/registro", respuesta)
                                intento_msg = f"Taxi {taxi_id} intentó registrarse pero ya ha culminado su jornada."
                                log_event(intento_msg)
                                with self.contadores_lock:
                                    self.global_counters['servicios_negados'] += 1
                                self.guardar_estado()
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
                    with taxis_info_lock, self.contadores_lock:
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
                                self.client.publish(f"taxis/{taxi_id}/fin_jornada", "Jornada laboral terminada. No puede aceptar más servicios.")
                        else:
                            disponible_msg = f"Taxi {taxi_id} intenta completar un servicio pero ya ha alcanzado su límite."
                            log_event(disponible_msg)
                            with self.contadores_lock:
                                self.global_counters['servicios_negados'] += 1
                            self.guardar_estado()

                elif topic_parts[2] == 'fin_jornada':
                    # Manejar notificación de fin de jornada del taxi
                    with taxis_info_lock, self.contadores_lock:
                        taxis_info[taxi_id]['servicios_completados'] = 3  # Forzar a que no pueda aceptar más servicios
                        taxis_info[taxi_id]['available'] = False  # Marcar como no disponible
                        fin_jornada_msg = f"Taxi {taxi_id} ha notificado el fin de su jornada laboral."
                        log_event(fin_jornada_msg)
                    self.guardar_estado()
                else:
                    desconocido_msg = f"Mensaje recibido en tópico desconocido: {msg.topic}"
                    log_event(desconocido_msg)
                    return

            except Exception as e:
                error_msg = f"Error al procesar mensaje: {e}"
                log_event(error_msg)
                with self.contadores_lock:
                    self.global_counters['servicios_negados'] += 1
                self.guardar_estado()
                return

        def switch_broker(self):
            """
            Función para cambiar al siguiente broker en la lista.
            """
            with self.lock:
                self.current_broker = (self.current_broker + 1) % len(self.brokers)
                broker = self.brokers[self.current_broker]
                try:
                    self.client.disconnect()
                except Exception as e:
                    log_event(f"Error al desconectar del broker actual: {e}")
                try:
                    self.client = mqtt.Client(client_id="Servidor", protocol=mqtt.MQTTv5)
                    self.client.on_connect = self.on_connect
                    self.client.on_disconnect = self.on_disconnect
                    self.client.on_message = self.on_message
                    self.client.connect(broker['address'], broker['port'], 60)
                    self.client.loop_start()
                    log_event(f"Intentando conectar al siguiente broker MQTT: {broker['address']}:{broker['port']}")
                except Exception as e:
                    log_event(f"No se pudo conectar al broker MQTT {broker['address']}:{broker['port']} - {e}")
                    # Intentar cambiar al siguiente broker después de un retraso
                    threading.Timer(5, self.switch_broker).start()

    mqtt_brokers = [
        {'address': '10.43.101.111', 'port': 1883},
        {'address': '10.43.100.114', 'port': 1883}
    ]

    # Instanciar el MQTTManager
    mqtt_manager = MQTTManager(mqtt_brokers, global_counters, contadores_lock, guardar_estado)

    def manejar_solicitudes_zmq():
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
                            global_counters['servicios_negados'] += 1
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
                                        global_counters['total_servicios'] += 1

                                    # Marcar al taxi como no disponible
                                    taxis_info[taxi_seleccionado]['available'] = False

                                    # Notificar al taxi del servicio asignado usando MQTTManager
                                    topic_servicio = f"taxis/{taxi_seleccionado}/servicio"
                                    mensaje_servicio = f"Usuario {user_id}, {user_x} {user_y}"
                                    mqtt_manager.client.publish(topic_servicio, mensaje_servicio)
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
                            global_counters['servicios_negados'] += 1
                        guardar_estado()

            except zmq.ContextTerminated:
                # Contexto terminado, salir del loop
                break
            except Exception as e:
                error_msg = f"Error al manejar solicitud ZeroMQ: {e}"
                log_event(error_msg)
                with contadores_lock:
                    global_counters['servicios_negados'] += 1
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
        if mqtt_manager.client:
            mqtt_manager.client.loop_stop()
            mqtt_manager.client.disconnect()
        socket_zmq.close()
        context.term()

        # Registrar resumen de servicios al cerrar
        guardar_estado()

        # Añadir mensaje de cierre sin timestamp
        with log_lock:
            try:
                with open(log_file, 'a') as f:
                    f.write(f"Servidor detenido\n")
            except Exception as e:
                log_event(f"Error al escribir en el archivo de registro durante el cierre: {e}")

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
        zmq_port=5555,
        intervalo_guardado=args.intervalo_guardado
    )
