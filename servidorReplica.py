"""
Uso:
    python server_replica.py --cuadricula_N <N> --cuadricula_M <M> [--intervalo_guardado <segundos>]

Ejemplo:
    python server_replica.py --cuadricula_N 50 --cuadricula_M 50 --intervalo_guardado 20
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
import json
import os


class ServidorReplica:
    def __init__(self, N, M, mqtt_broker_address, mqtt_broker_port, zmq_port, intervalo_guardado):
        """
        Inicializa el servidor réplica con los parámetros especificados.
        """
        self.N = N
        self.M = M
        self.mqtt_broker_address = mqtt_broker_address
        self.mqtt_broker_port = mqtt_broker_port
        self.zmq_port = zmq_port
        self.intervalo_guardado = intervalo_guardado

        # Estructuras de datos para almacenar información de los taxis
        self.taxis_info = defaultdict(lambda: {
            'initial_pos': None,
            'positions': [],
            'servicios_completados': 0,
            'servicios_asignados': [],  # Lista de tuplas: (Pos Taxi, Pos Usuario)
            'available': True  # Bandera de disponibilidad
        })
        self.taxis_info_lock = threading.Lock()

        # Contadores globales
        self.total_servicios = 0  # Servicios asignados
        self.servicios_negados = 0
        self.contadores_lock = threading.Lock()

        # Configuración de ZeroMQ para manejar solicitudes de usuarios
        self.context = zmq.Context()
        self.socket_zmq = self.context.socket(zmq.REP)
        try:
            self.socket_zmq.bind(f"tcp://*:{self.zmq_port}")
            self.log_event(f"Servidor Réplica ZeroMQ escuchando en puerto {self.zmq_port}")
        except zmq.ZMQError as e:
            self.log_event(f"Error al enlazar ZeroMQ en puerto {self.zmq_port}: {e}")
            sys.exit(1)

        # Evento para detener los hilos correctamente
        self.stop_event = threading.Event()

        # Archivo de sincronización (servidor réplica)
        self.archivo_sincronizacion = "Interaccion_replica.txt"

        # Configuración del cliente MQTT
        self.client = mqtt.Client(client_id="Servidor_Réplica", protocol=mqtt.MQTTv5)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def log_event(self, message):
        """
        Función para imprimir mensajes en consola.

        Parámetros:
            message (str): Mensaje a imprimir.
        """
        print(message)

    def guardar_estado(self):
        """
        Función para guardar el estado actual del sistema en el archivo de sincronización.
        """
        with self.taxis_info_lock, self.contadores_lock:
            try:
                # Convertir defaultdict a dict para evitar problemas al serializar
                taxis_info_serializable = {taxi_id: info for taxi_id, info in self.taxis_info.items()}
                estado = {
                    'taxis_info': taxis_info_serializable,
                    'total_servicios': self.total_servicios,
                    'servicios_negados': self.servicios_negados
                }
                with open(self.archivo_sincronizacion, 'w') as f:
                    json.dump(estado, f, indent=4)
                self.log_event("Estado actual del sistema guardado en Interaccion_replica.txt")
            except Exception as e:
                self.log_event(f"Error al guardar el estado: {e}")

    def cargar_estado_replica(self):
        """
        Función para cargar el estado del sistema desde el archivo de sincronización Interaccion_replica.txt.
        """
        try:
            with open(self.archivo_sincronizacion, 'r') as f:
                estado = json.load(f)
            with self.taxis_info_lock, self.contadores_lock:
                self.taxis_info.clear()
                for taxi_id, info in estado.get('taxis_info', {}).items():
                    self.taxis_info[int(taxi_id)] = info
                self.total_servicios = estado.get('total_servicios', 0)
                self.servicios_negados = estado.get('servicios_negados', 0)
            self.log_event("Estado del sistema sincronizado desde Interaccion_replica.txt.")
        except FileNotFoundError:
            self.log_event("Archivo Interaccion_replica.txt no encontrado para sincronización.")
        except json.JSONDecodeError as e:
            self.log_event(f"Error al decodificar JSON: {e}")
        except Exception as e:
            self.log_event(f"Error al cargar el estado: {e}")

    def sincronizar_con_archivo(self):
        """
        Función para sincronizar el estado del servidor réplica leyendo Interaccion_replica.txt periódicamente.
        """
        last_mod_time = 0
        while not self.stop_event.is_set():
            try:
                if os.path.exists(self.archivo_sincronizacion):
                    current_mod_time = os.path.getmtime(self.archivo_sincronizacion)
                    if current_mod_time != last_mod_time:
                        self.cargar_estado_replica()
                        last_mod_time = current_mod_time
                else:
                    self.log_event("Archivo Interaccion_replica.txt no encontrado para sincronización.")
            except Exception as e:
                self.log_event(f"Error al sincronizar con Interaccion_replica.txt: {e}")
            time.sleep(5)  # Intervalo de sincronización en segundos

    def on_connect(self, client, userdata, flags, rc, properties=None):
        """
        Callback cuando el cliente MQTT se conecta al broker.
        """
        if rc == 0:
            self.log_event("Servidor Réplica MQTT conectado al broker exitosamente.")
            # Suscribirse a todos los tópicos de posición de taxis y completados
            client.subscribe("taxis/+/posicion")
            client.subscribe("taxis/+/completado")
            client.subscribe("taxis/+/fin_jornada")
            # Suscribirse al tópico donde el servidor principal publica Interaccion.txt
            client.subscribe("servidor/Interaccion")
        else:
            error_msg = f"Error al conectar al broker MQTT, código de retorno {rc}"
            self.log_event(error_msg)
            with self.contadores_lock:
                self.servicios_negados += 1
            self.guardar_estado()
            sys.exit(1)

    def on_message(self, client, userdata, msg):
        """
        Callback cuando se recibe un mensaje MQTT.
        """
        try:
            if msg.topic == "servidor/Interaccion":
                # Mensaje de sincronización de estado desde el servidor principal
                contenido = msg.payload.decode()
                if not contenido.strip():
                    self.log_event("Mensaje vacío recibido en 'servidor/Interaccion'.")
                    return
                try:
                    estado = json.loads(contenido)
                    with self.taxis_info_lock, self.contadores_lock:
                        self.taxis_info.clear()
                        for taxi_id, info in estado.get('taxis_info', {}).items():
                            self.taxis_info[int(taxi_id)] = info
                        self.total_servicios = estado.get('total_servicios', 0)
                        self.servicios_negados = estado.get('servicios_negados', 0)
                    self.log_event("Estado del sistema sincronizado desde 'servidor/Interaccion'.")
                except json.JSONDecodeError as e:
                    self.log_event(f"Error al decodificar JSON: {e}")
                return

            # Procesar mensajes de taxis como en el servidor principal
            topic_parts = msg.topic.split('/')
            if len(topic_parts) < 3 or topic_parts[0] != 'taxis':
                desconocido_msg = f"Mensaje recibido en tópico desconocido: {msg.topic}"
                self.log_event(desconocido_msg)
                return

            try:
                taxi_id = int(topic_parts[1])
            except ValueError:
                self.log_event(f"Taxi ID no válido en el tópico: {msg.topic}")
                return

            if topic_parts[2] == 'posicion':
                # Manejar actualización de posición
                payload = msg.payload.decode().strip().split()
                if len(payload) != 2:
                    self.log_event(f"Formato de posición incorrecto para Taxi {taxi_id}: {msg.payload.decode()}")
                    return
                x_str, y_str = payload
                try:
                    x = int(x_str)
                    y = int(y_str)
                except ValueError:
                    self.log_event(f"Valores de posición no válidos para Taxi {taxi_id}: {msg.payload.decode()}")
                    return

                with self.taxis_info_lock:
                    if self.taxis_info[taxi_id]['initial_pos'] is None:
                        # Registro inicial del taxi
                        if self.taxis_info[taxi_id]['servicios_completados'] >= 3:
                            # El taxi ha alcanzado su límite de servicios
                            respuesta = "ID Taxi inactivo. No puede registrarse."
                            client.publish(f"taxis/{taxi_id}/registro", respuesta)
                            intento_msg = f"Taxi {taxi_id} intentó registrarse pero ya ha culminado su jornada."
                            self.log_event(intento_msg)
                            with self.contadores_lock:
                                self.servicios_negados += 1
                            self.guardar_estado()
                            return
                        self.taxis_info[taxi_id]['initial_pos'] = (x, y)
                        self.taxis_info[taxi_id]['positions'].append((x, y))
                        # Registrar en el archivo
                        registro_msg = f"**Nuevo Taxi Registrado: ID {taxi_id} en posición ({x}, {y})**"
                        self.log_event(registro_msg)
                    else:
                        # Actualización de posición
                        self.taxis_info[taxi_id]['positions'].append((x, y))
                        actualizacion_msg = f"Posición actual del Taxi {taxi_id}: ({x}, {y})"
                        self.log_event(actualizacion_msg)

            elif topic_parts[2] == 'completado':
                # Manejar completación de servicio
                with self.taxis_info_lock, self.contadores_lock:
                    if self.taxis_info[taxi_id]['servicios_completados'] < 3:
                        self.taxis_info[taxi_id]['servicios_completados'] += 1
                        completado_msg = f"Taxi {taxi_id} ha completado un servicio. Total servicios: {self.taxis_info[taxi_id]['servicios_completados']}"
                        self.log_event(completado_msg)

                        # Marcar al taxi como disponible nuevamente
                        self.taxis_info[taxi_id]['available'] = True

                        if self.taxis_info[taxi_id]['servicios_completados'] == 3:
                            limite_msg = f"Taxi {taxi_id} ha alcanzado el límite de servicios. No podrá aceptar más servicios."
                            self.log_event(limite_msg)
                            # Notificar al taxi que su jornada ha terminado
                            client.publish(f"taxis/{taxi_id}/fin_jornada", "Jornada laboral culminada. No puede aceptar más servicios.")
                    else:
                        disponible_msg = f"Taxi {taxi_id} intenta completar un servicio pero ya ha alcanzado su límite."
                        self.log_event(disponible_msg)
                        with self.contadores_lock:
                            self.servicios_negados += 1
                        self.guardar_estado()

            elif topic_parts[2] == 'fin_jornada':
                # Manejar notificación de fin de jornada del taxi
                try:
                    with self.taxis_info_lock, self.contadores_lock:
                        if taxi_id not in self.taxis_info:
                            self.log_event(f"Error: Taxi {taxi_id} no registrado en taxis_info.")
                            return

                        self.taxis_info[taxi_id]['servicios_completados'] = 3  # Forzar a que no pueda aceptar más servicios
                        self.taxis_info[taxi_id]['available'] = False  # Marcar como no disponible
                        fin_jornada_msg = f"Taxi {taxi_id} ha notificado el fin de su jornada laboral."
                        self.log_event(fin_jornada_msg)
                    self.guardar_estado()
                except KeyError as e:
                    self.log_event(f"Error: Clave no encontrada en taxis_info: {e}")
                except IOError as e:
                    self.log_event(f"Error de E/S al guardar el estado: {e}")
                except Exception as e:
                    self.log_event(f"Error inesperado al manejar 'fin_jornada' para Taxi {taxi_id}: {e}")
            else:
                desconocido_msg = f"Mensaje recibido en tópico desconocido: {msg.topic}"
                self.log_event(desconocido_msg)
                return
        except Exception as e:
            # Manejo de cualquier otro error inesperado
            self.log_event(f"Error inesperado al procesar mensaje en el tópico {msg.topic}: {e}")

    def manejar_solicitudes_zmq(self):
        """
        Manejar solicitudes de usuarios vía ZeroMQ con un timeout de 60 segundos.
        """
        while not self.stop_event.is_set():
            try:
                # Usar poller para permitir la salida del loop
                poller = zmq.Poller()
                poller.register(self.socket_zmq, zmq.POLLIN)
                socks = dict(poller.poll(1000))  # Timeout de 1 segundo

                if self.socket_zmq in socks and socks[self.socket_zmq] == zmq.POLLIN:
                    mensaje_recibido = self.socket_zmq.recv_string()
                    try:
                        user_id, user_x, user_y = map(int, mensaje_recibido.strip().split(','))
                    except ValueError:
                        self.log_event(f"Formato de solicitud incorrecto: {mensaje_recibido}")
                        self.socket_zmq.send_string("Formato de solicitud incorrecto.")
                        with self.contadores_lock:
                            self.servicios_negados += 1
                        self.guardar_estado()
                        continue

                    solicitud_msg = f"{user_id}\t({user_x},{user_y})"
                    self.log_event(f"Recibida solicitud de Taxi para Usuario {user_id} en posición ({user_x}, {user_y})")
                    start_time = time.time()

                    asignacion_exitosa = False

                    while time.time() - start_time < 60:
                        with self.taxis_info_lock:
                            # Filtrar taxis disponibles dentro de la cuadrícula, que no han alcanzado su límite de servicios y están disponibles
                            taxis_disponibles = [
                                taxi for taxi, info in self.taxis_info.items()
                                if info['servicios_completados'] < 3 and info['initial_pos'] is not None and info['available']
                            ]
                            if taxis_disponibles:
                                # Seleccionar el taxi más cercano usando la distancia de Manhattan
                                taxi_seleccionado = None
                                distancia_min = float('inf')
                                pos_usuario = (user_x, user_y)
                                for taxi in taxis_disponibles:
                                    pos_taxi = self.taxis_info[taxi]['positions'][-1]  # Última posición del taxi
                                    distancia = abs(pos_taxi[0] - user_x) + abs(pos_taxi[1] - user_y)
                                    if distancia < distancia_min:
                                        distancia_min = distancia
                                        taxi_seleccionado = taxi

                                if taxi_seleccionado is not None:
                                    # Asignar el taxi seleccionado
                                    respuesta = f"OK {taxi_seleccionado}"
                                    self.socket_zmq.send_string(respuesta)
                                    asignacion_exitosa = True

                                    asignacion_msg = f"Asignado Taxi {taxi_seleccionado} al Usuario {user_id}: Desde ({self.taxis_info[taxi_seleccionado]['positions'][-1][0]}, {self.taxis_info[taxi_seleccionado]['positions'][-1][1]}) hacia ({user_x}, {user_y})"
                                    self.log_event(asignacion_msg)

                                    # Registrar la asignación
                                    self.taxis_info[taxi_seleccionado]['servicios_asignados'].append(
                                        ((self.taxis_info[taxi_seleccionado]['positions'][-1][0],
                                          self.taxis_info[taxi_seleccionado]['positions'][-1][1]),
                                         (user_x, user_y))
                                    )

                                    # Incrementar contador de servicios exitosos
                                    with self.contadores_lock:
                                        self.total_servicios += 1

                                    # Marcar al taxi como no disponible
                                    self.taxis_info[taxi_seleccionado]['available'] = False

                                    # Notificar al taxi del servicio asignado
                                    topic_servicio = f"taxis/{taxi_seleccionado}/servicio"
                                    mensaje_servicio = f"Usuario {user_id}, {user_x} {user_y}"
                                    self.client.publish(topic_servicio, mensaje_servicio)
                                    self.log_event(f"Notificado al Taxi {taxi_seleccionado} sobre el servicio asignado al Usuario {user_id}.")

                                    break  # Salir del loop de espera

                        if asignacion_exitosa:
                            self.guardar_estado()
                            break

                        time.sleep(1)  # Esperar 1 segundo antes de volver a intentar

                    if not asignacion_exitosa:
                        # No se encontró ningún taxi disponible después de 60 segundos
                        respuesta = "NO Taxi disponibles en este momento."
                        self.socket_zmq.send_string(respuesta)
                        negado_msg = f"{user_id}\tTimeout: No se asignó un taxi en 60 segundos."
                        self.log_event(negado_msg)
                        with self.contadores_lock:
                            self.servicios_negados += 1
                        self.guardar_estado()

            except zmq.ContextTerminated:
                # Contexto terminado, salir del loop
                break
            except Exception as e:
                error_msg = f"Error al manejar solicitud ZeroMQ: {e}"
                self.log_event(error_msg)
                with self.contadores_lock:
                    self.servicios_negados += 1
                self.guardar_estado()

    def guardar_estado_periodicamente(self):
        """
        Hilo que guarda el estado periódicamente según el intervalo especificado.
        """
        while not self.stop_event.is_set():
            self.guardar_estado()
            time.sleep(self.intervalo_guardado)

    def shutdown(self, signum, frame):
        """
        Función para manejar el cierre del servidor réplica de manera ordenada.
        """
        self.log_event("\nServidor Réplica detenido.")
        self.stop_event.set()
        self.guardar_estado()
        # Añadir mensaje de cierre con timestamp
        try:
            with open(self.archivo_sincronizacion, 'a') as f:
                f.write(f"Servidor Réplica detenido en {datetime.now()}\n")
            self.log_event("Resumen de servicios registrado en Interaccion_replica.txt")
        except Exception as e:
            self.log_event(f"Error al escribir el mensaje de cierre: {e}")
        # Cerrar conexiones y hilos
        self.client.loop_stop()
        self.client.disconnect()
        self.socket_zmq.close()
        self.context.term()
        sys.exit(0)

    def iniciar(self):
        """
        Inicia el servidor réplica.
        """
        # Conectar al broker MQTT
        try:
            self.client.connect(self.mqtt_broker_address, self.mqtt_broker_port, 60)
            self.log_event("Servidor Réplica MQTT conectado al broker exitosamente.")
        except Exception as e:
            error_msg = f"No se pudo conectar al broker MQTT: {e}"
            self.log_event(error_msg)
            with self.contadores_lock:
                self.servicios_negados += 1
            self.guardar_estado()
            sys.exit(1)

        self.client.loop_start()

        # Iniciar el hilo para manejar solicitudes de usuarios
        self.thread_zmq = threading.Thread(target=self.manejar_solicitudes_zmq, daemon=True)
        self.thread_zmq.start()

        # Iniciar el hilo para sincronizar con Interaccion_replica.txt
        self.hilo_sincronizacion = threading.Thread(target=self.sincronizar_con_archivo, daemon=True)
        self.hilo_sincronizacion.start()

        # Iniciar el hilo para guardar el estado periódicamente
        self.hilo_guardado = threading.Thread(target=self.guardar_estado_periodicamente, daemon=True)
        self.hilo_guardado.start()

        # Manejar señales de interrupción para cerrar el servidor correctamente
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        # Mantener el hilo principal activo
        while not self.stop_event.is_set():
            time.sleep(1)


def main():
    parser = argparse.ArgumentParser(description="Proceso del Servidor Réplica")
    parser.add_argument('--cuadricula_N', type=int, required=True, help='Tamaño N de la cuadrícula')
    parser.add_argument('--cuadricula_M', type=int, required=True, help='Tamaño M de la cuadrícula')
    parser.add_argument('--intervalo_guardado', type=int, default=60, help='Intervalo en segundos para guardar el estado (default: 60)')
    args = parser.parse_args()

    replica = ServidorReplica(
        N=args.cuadricula_N,
        M=args.cuadricula_M,
        mqtt_broker_address='test.mosquitto.org',  # Usar el mismo broker MQTT que el servidor principal
        mqtt_broker_port=1883,
        zmq_port=5556,  # Puerto para la réplica
        intervalo_guardado=args.intervalo_guardado
    )

    replica.iniciar()


if __name__ == "__main__":
    main()
