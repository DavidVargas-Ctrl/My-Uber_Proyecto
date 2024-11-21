"""
Uso:
    python taxi.py --taxi_id <id> --cuadricula_N <N> --cuadricula_M <M> --init_x <x> --init_y <y> --velocidad <velocidad>

Ejemplos:
    python3 taxi.py --taxi_id 1 --cuadricula_N 50 --cuadricula_M 50 --init_x 5 --init_y 5 --velocidad 2
    python3 taxi.py --taxi_id 3 --cuadricula_N 50 --cuadricula_M 50 --init_x 6 --init_y 7 --velocidad 4
    python3 taxi.py --taxi_id 2 --cuadricula_N 50 --cuadricula_M 50 --init_x 6 --init_y 7 --velocidad 1
    python3 taxi.py --taxi_id 4 --cuadricula_N 50 --cuadricula_M 50 --init_x 45 --init_y 37 --velocidad 4
"""

import time
import threading
import random
import argparse
import sys
import paho.mqtt.client as mqtt


class MQTTManager:
    def __init__(self, brokers, taxi_id, on_service_message, on_connected, on_disconnected):
        """
        Inicializa el gestor de MQTT con una lista de brokers.

        Parámetros:
            brokers (list): Lista de diccionarios con 'address' y 'port' de los brokers MQTT.
            taxi_id (int): Identificador único del taxi.
            on_service_message (function): Callback para manejar mensajes de servicio.
            on_connected (function): Callback cuando se conecta al broker.
            on_disconnected (function): Callback cuando se desconecta del broker.
        """
        self.brokers = brokers
        self.current_broker = 0
        self.client = None
        self.lock = threading.Lock()
        self.on_service_message = on_service_message
        self.on_connected = on_connected
        self.on_disconnected = on_disconnected
        self.taxi_id = taxi_id
        self.connect_to_broker()

    def connect_to_broker(self):
        """
        Conecta al broker MQTT actual.
        """
        with self.lock:
            broker = self.brokers[self.current_broker]
            self.client = mqtt.Client(client_id=f"Taxi_{self.taxi_id}", protocol=mqtt.MQTTv5)
            self.client.on_connect = self.handle_connect
            self.client.on_disconnect = self.handle_disconnect
            self.client.on_message = self.handle_message

            try:
                self.client.connect(broker['address'], broker['port'], 60)
                self.client.loop_start()
                print(f"Taxi {self.taxi_id}: Intentando conectar al broker MQTT: {broker['address']}:{broker['port']}")
            except Exception as e:
                print(f"Taxi {self.taxi_id}: No se pudo conectar al broker MQTT {broker['address']}:{broker['port']} - {e}")
                self.switch_broker()

    def handle_connect(self, client, userdata, flags, rc, properties=None):
        """
        Callback cuando el cliente se conecta al broker MQTT.
        """
        if rc == 0:
            broker = self.brokers[self.current_broker]
            print(f"Taxi {self.taxi_id}: Conectado al broker MQTT {broker['address']}:{broker['port']}")
            # Suscribirse al tópico de servicios
            topic_servicio = f"taxis/{self.taxi_id}/servicio"
            self.client.subscribe(topic_servicio)
            print(f"Taxi {self.taxi_id}: Suscrito al tópico {topic_servicio}")
            self.on_connected()
        else:
            print(f"Taxi {self.taxi_id}: Error al conectar al broker MQTT, código de retorno {rc}")
            self.on_disconnected()
            self.switch_broker()

    def handle_disconnect(self, client, userdata, rc, properties=None):
        """
        Callback cuando el cliente se desconecta del broker MQTT.
        """
        broker = self.brokers[self.current_broker]
        print(f"Taxi {self.taxi_id}: Desconectado del broker MQTT {broker['address']}:{broker['port']} con código {rc}")
        self.on_disconnected()
        if rc != 0:
            self.switch_broker()

    def handle_message(self, client, userdata, msg):
        """
        Callback cuando se recibe un mensaje en un tópico suscrito.
        """
        if msg.topic == f"taxis/{self.taxi_id}/servicio":
            self.on_service_message(msg)

    def publish(self, topic, message):
        """
        Publica un mensaje en un tópico MQTT.

        Parámetros:
            topic (str): Tópico en el que publicar.
            message (str): Mensaje a publicar.
        """
        try:
            self.client.publish(topic, message)
            print(f"Taxi {self.taxi_id}: Publicado en {topic}: {message}")
        except Exception as e:
            print(f"Taxi {self.taxi_id}: Error al publicar en {topic}: {e}")

    def switch_broker(self):
        """
        Cambia al siguiente broker en la lista.
        """
        with self.lock:
            self.current_broker = (self.current_broker + 1) % len(self.brokers)
            broker = self.brokers[self.current_broker]
            print(f"Taxi {self.taxi_id}: Cambiando al siguiente broker MQTT: {broker['address']}:{broker['port']}")
            try:
                self.client.disconnect()
            except Exception as e:
                print(f"Taxi {self.taxi_id}: Error al desconectar del broker actual: {e}")
            # Intentar conectar al siguiente broker después de un breve retraso
            threading.Timer(5, self.connect_to_broker).start()


def taxi_procesos(taxi_id, tam_cuadricula, pos_inicial, velocidad_kmh, brokers):
    """
    Función principal para el proceso del taxi.

    Parámetros:
        taxi_id (int): Identificador único del taxi.
        tam_cuadricula (tuple): Dimensiones de la cuadrícula (N, M).
        pos_inicial (tuple): Posición inicial (x, y) del taxi.
        velocidad_kmh (int): Velocidad de movimiento en km/h (1, 2 o 4).
        brokers (list): Lista de diccionarios con 'address' y 'port' de los brokers MQTT.
    """

    N, M = tam_cuadricula
    x, y = pos_inicial  # Posición actual del taxi
    x_initial, y_initial = pos_inicial  # Guardar posición inicial para regresar
    servicios_completados = 0  # Contador de servicios
    max_servicios = 3  # Límite de servicios por jornada
    tiempo_total = 0  # Tiempo acumulado del taxi en minutos

    # Independencia entre hilos
    lock = threading.Lock()

    objetivo_usuario = None  # (user_id, user_x, user_y)
    movimiento_usuario_event = threading.Event()

    def on_service_message(msg):
        nonlocal servicios_completados, objetivo_usuario, tiempo_total
        try:
            # Mensaje de servicio: "Usuario {user_id}, {x} {y}"
            mensaje = msg.payload.decode()
            partes = mensaje.split(',')
            if len(partes) != 2:
                print(f"Taxi {taxi_id} recibió mensaje de manera incorrecta: {mensaje}")
                return
            user_id = partes[0].strip().split()[1]
            user_coords = partes[1].strip().split()
            if len(user_coords) != 2:
                print(f"Taxi {taxi_id} recibió coordenadas malformadas: {partes[1]}")
                return
            user_x = int(user_coords[0])
            user_y = int(user_coords[1])

            # Notificar que está recogiendo al usuario
            mover_al_usuario(user_id, user_x, user_y)
        except Exception as e:
            print(f"Taxi {taxi_id} error al procesar mensaje de servicio: {e}")

    def on_connected():
        print(f"Taxi {taxi_id}: Conexión MQTT establecida.")

    def on_disconnected():
        print(f"Taxi {taxi_id}: Conexión MQTT perdida.")

    # Instanciar el MQTTManager
    mqtt_manager = MQTTManager(
        brokers=brokers,
        taxi_id=taxi_id,
        on_service_message=on_service_message,
        on_connected=on_connected,
        on_disconnected=on_disconnected
    )

    def mover_al_usuario(user_id, user_x, user_y):
        """
        Notificar al usuario que el taxi está recogiendo.

        Parámetros:
            user_id (str): Identificador del usuario.
            user_x (int): Posición x del usuario.
            user_y (int): Posición y del usuario.
        """
        nonlocal servicios_completados, objetivo_usuario, tiempo_total
        with lock:
            if servicios_completados >= max_servicios:
                print(f"Taxi {taxi_id}: No puede aceptar más servicios. Jornada laboral culminada.")
                return

            if objetivo_usuario is not None:
                print(f"Taxi {taxi_id}: Ya está atendiendo una solicitud. Ignorando nueva solicitud de Usuario {user_id}.")
                return  # Ya está atendiendo a un usuario

            objetivo_usuario = (user_id, user_x, user_y)
            print(f"Taxi {taxi_id}: Está recogiendo al Usuario {user_id} en ({user_x}, {user_y})")
            movimiento_usuario_event.set()

    def publicar_posicion():
        """ Publica la posición actual del taxi al broker MQTT. """
        mensaje = f"{x} {y}"
        mqtt_manager.publish(f"taxis/{taxi_id}/posicion", mensaje)

    print(f"Taxi {taxi_id}: Se ha registrado exitosamente en el servidor.")
    print(f"Taxi {taxi_id}: Posición inicial: ({x}, {y})")
    publicar_posicion()

    def movimiento_func():
        nonlocal x, y, servicios_completados, objetivo_usuario, tiempo_total

        while True:
            if movimiento_usuario_event.is_set():
                # Notificar recogida al usuario
                user_id, user_x, user_y = objetivo_usuario
                print(f"Taxi {taxi_id}: Está recogiendo al Usuario {user_id} en ({user_x}, {user_y})")

                # Simular tiempo de servicio (30 segundos reales = 30 minutos simulados)
                tiempo_servicio = 30  # segundos reales
                print(f"Taxi {taxi_id}: Realizando servicio al Usuario {user_id} por {tiempo_servicio} minutos simulados.")
                time.sleep(tiempo_servicio)  # Dormir durante el servicio
                tiempo_total += 30  # Incrementar en 30 minutos

                # Notificar al servidor que el servicio ha sido completado
                mensaje_completado = f"Taxi {taxi_id} ha completado el servicio para Usuario {user_id}."
                mqtt_manager.publish(f"taxis/{taxi_id}/completado", mensaje_completado)
                print(f"Taxi {taxi_id}: Ha completado el servicio para Usuario {user_id}. Tiempo total: {tiempo_total} minutos.")

                # Incrementar el contador de servicios
                servicios_completados += 1

                if servicios_completados >= max_servicios:
                    # Notificar al servidor que la jornada ha finalizado
                    mensaje_fin_jornada = "Jornada laboral culminada. No puede aceptar más servicios."
                    mqtt_manager.publish(f"taxis/{taxi_id}/fin_jornada", mensaje_fin_jornada)
                    print(f"Taxi {taxi_id}: Ha alcanzado el límite de servicios. Notificando fin de jornada.")
                    movimiento_usuario_event.clear()
                    objetivo_usuario = None
                    break  # Detener el hilo de movimiento

                # Retornar inmediatamente a la posición inicial
                print(f"Taxi {taxi_id}: Regresando a la posición inicial ({x_initial}, {y_initial}).")
                with lock:
                    x, y = x_initial, y_initial  # Retornar a la posición inicial
                publicar_posicion()
                print(f"Taxi {taxi_id}: Ha regresado a la posición inicial ({x}, {y}). Tiempo total: {tiempo_total} minutos.")

                # Resetear el objetivo
                objetivo_usuario = None
                movimiento_usuario_event.clear()

            else:
                # Movimiento aleatorio según la velocidad
                with lock:
                    # Definir celdas a mover y tiempo de espera basado en la velocidad
                    if velocidad_kmh == 1:
                        celdas_a_mover = 1
                        sleep_time = 60
                    elif velocidad_kmh == 2:
                        celdas_a_mover = 1
                        sleep_time = 30
                    elif velocidad_kmh == 4:
                        celdas_a_mover = 2
                        sleep_time = 30
                    else:
                        celdas_a_mover = 1
                        sleep_time = 30  # Valor por defecto

                    for _ in range(celdas_a_mover):
                        dx, dy = random.choice([(0, 1), (0, -1), (1, 0), (-1, 0)])  # Movimiento aleatorio
                        nuevo_x, nuevo_y = x + dx, y + dy

                        # Validar límites de la cuadrícula
                        if 0 <= nuevo_x <= N and 0 <= nuevo_y <= M:
                            x, y = nuevo_x, nuevo_y

                # Esperar el intervalo antes de mover nuevamente
                time.sleep(sleep_time)  # Sleep según la velocidad
                tiempo_total += sleep_time  # Incrementar en minutos simulados
                publicar_posicion()
                print(f"Taxi {taxi_id}: Han transcurrido {tiempo_total} minutos. Posición del taxi: ({x}, {y}).")

    # Iniciar el hilo de movimiento
    hilo_movimiento = threading.Thread(target=movimiento_func, daemon=True)
    hilo_movimiento.start()

    # Mantener el hilo principal activo
    try:
        while hilo_movimiento.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"Taxi {taxi_id}: Detenido por el usuario.")
        mqtt_manager.client.loop_stop()
        mqtt_manager.client.disconnect()
        sys.exit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Proceso del Taxi")
    parser.add_argument('--taxi_id', type=int, required=True, help='Identificador único del taxi')
    parser.add_argument('--cuadricula_N', type=int, required=True, help='Tamaño N de la cuadrícula')
    parser.add_argument('--cuadricula_M', type=int, required=True, help='Tamaño M de la cuadrícula')
    parser.add_argument('--init_x', type=int, required=True, help='Posición inicial x')
    parser.add_argument('--init_y', type=int, required=True, help='Posición inicial y')
    parser.add_argument('--velocidad', type=int, required=True, choices=[1, 2, 4], help='Velocidad en km/h (1, 2, 4)')
    args = parser.parse_args()


    brokers_list = [
        {'address': '10.43.101.111', 'port': 1883},
        {'address': '10.43.100.114', 'port': 1883}
    ]

    taxi_procesos(
        taxi_id=args.taxi_id,
        tam_cuadricula=(args.cuadricula_N, args.cuadricula_M),
        pos_inicial=(args.init_x, args.init_y),
        velocidad_kmh=args.velocidad,
        brokers=brokers_list
    )
