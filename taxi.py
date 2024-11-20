"""
Uso:
    python taxi.py --taxi_id <id> --cuadricula_N <N> --cuadricula_M <M> --init_x <x> --init_y <y> --velocidad <velocidad> --port <port>

Ejemplos:
    python taxi.py --taxi_id 1 --cuadricula_N 50 --cuadricula_M 50 --init_x 5 --init_y 5 --velocidad 2 --port 1883
    python taxi.py --taxi_id 3 --cuadricula_N 50 --cuadricula_M 50 --init_x 6 --init_y 7 --velocidad 4 --port 1883
    python taxi.py --taxi_id 2 --cuadricula_N 50 --cuadricula_M 50 --init_x 6 --init_y 7 --velocidad 1 --port 1883
    python taxi.py --taxi_id 4 --cuadricula_N 50 --cuadricula_M 50 --init_x 45 --init_y 37 --velocidad 4 --port 1883
"""


import time
import threading
import random
import argparse
import sys
import paho.mqtt.client as mqtt

def taxi_procesos(taxi_id, tam_cuadricula, pos_inicial, velocidad_kmh, broker_addresses, broker_port):
    """
    Función principal para el proceso del taxi.

    Parámetros:
        taxi_id (int): Identificador único del taxi.
        tam_cuadricula (tuple): Dimensiones de la cuadrícula (N, M).
        pos_inicial (tuple): Posición inicial (x, y) del taxi.
        velocidad_kmh (int): Velocidad de movimiento en km/h (1, 2 o 4).
        broker_addresses (list): Lista de direcciones del broker MQTT.
        broker_port (int): Puerto del broker MQTT.
    """

    N, M = tam_cuadricula
    x, y = pos_inicial  # Posición actual del taxi
    x_initial, y_initial = pos_inicial  # Guardar posición inicial para regresar
    servicios_completados = 0  # Contador de servicios
    max_servicios = 3  # Límite de servicios por jornada
    tiempo_total = 0  # Tiempo acumulado del taxi en minutos

    # Independencia entre hilos
    lock = threading.Lock()

    # Configuración del cliente MQTT
    client = mqtt.Client(client_id=f"Taxi_{taxi_id}")

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            topic_servicio = f"taxis/{taxi_id}/servicio"
            client.subscribe(topic_servicio)
            print(f"Taxi {taxi_id} conectado al broker MQTT y suscrito a {topic_servicio}")
        else:
            print(f"Taxi {taxi_id} error al conectar al broker MQTT, código de retorno {rc}")
            reconnect_to_next_broker(client)

    def reconnect_to_next_broker(client):
        """
        Intentar conectarse al siguiente broker en la lista.
        """
        nonlocal current_broker_index
        current_broker_index += 1
        if current_broker_index < len(broker_addresses):
            next_broker = broker_addresses[current_broker_index]
            print(f"Intentando conectar al siguiente broker MQTT: {next_broker}")
            try:
                client.connect(next_broker, broker_port, 60)
            except Exception as e:
                print(f"No se pudo conectar al broker MQTT: {e}")
                reconnect_to_next_broker(client)
        else:
            print("No hay más brokers disponibles para intentar.")
            sys.exit(1)

    def on_message(client, userdata, msg):
        try:
            topic_parts = msg.topic.split('/')
            if len(topic_parts) != 3 or topic_parts[0] != 'taxis' or topic_parts[2] != 'servicio':
                return

            # Mensaje de servicio: "Usuario {user_id}, {x} {y}"
            mensaje = msg.payload.decode()
            partes = mensaje.split(',')
            if len(partes) != 2:
                print(f"Taxi {taxi_id} recibió mensaje malformado: {mensaje}")
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
                print(f"Taxi {taxi_id} no puede aceptar más servicios. Jornada laboral culminada.")
                return

            if objetivo_usuario is not None:
                print(f"Taxi {taxi_id} ya está atendiendo una solicitud. Ignorando nueva solicitud de Usuario {user_id}.")
                return  # Ya está atendiendo a un usuario

            objetivo_usuario = (user_id, user_x, user_y)
            print(f"Taxi {taxi_id} está recogiendo al Usuario {user_id} en ({user_x}, {user_y})")
            movimiento_usuario_event.set()

    def publicar_posicion():
        """ Publica la posición actual del taxi al broker MQTT. """
        mensaje = f"{x} {y}"
        client.publish(topic_posicion, mensaje)
        print(f"Taxi {taxi_id} publicó posición: ({x}, {y})")

    # Asignar callbacks
    client.on_connect = on_connect
    client.on_message = on_message

    current_broker_index = 0

    try:
        client.connect(broker_addresses[current_broker_index], broker_port, 60)
    except Exception as e:
        print(f"No se pudo conectar al broker MQTT: {e}")
        reconnect_to_next_broker(client)

    client.loop_start()

    # Configuración del tópico al que se está publicando posiciones
    topic_posicion = f"taxis/{taxi_id}/posicion"

    # Variables para gestionar el movimiento hacia usuarios
    movimiento_usuario_event = threading.Event()
    objetivo_usuario = None  # (user_id, user_x, user_y)

    print(f"Taxi {taxi_id} se ha registrado exitosamente en el servidor.")
    print(f"Posición inicial del Taxi {taxi_id}: ({x}, {y})")
    publicar_posicion()

    def movimiento_func():
        nonlocal x, y, servicios_completados, objetivo_usuario, tiempo_total

        while True:
            if movimiento_usuario_event.is_set():
                # Notificar recogida al usuario
                user_id, user_x, user_y = objetivo_usuario
                print(f"Taxi {taxi_id} está recogiendo al Usuario {user_id} en ({user_x}, {user_y})")

                # Simular tiempo de servicio (30 segundos reales = 30 minutos simulados)
                tiempo_servicio = 30  # segundos reales
                print(f"Taxi {taxi_id} está realizando el servicio al Usuario {user_id} por {tiempo_servicio} minutos.")
                time.sleep(tiempo_servicio)  # Dormir durante el servicio
                tiempo_total += 30  # Incrementar en 30 minutos

                # Notificar al servidor que el servicio ha sido completado
                topic_completado = f"taxis/{taxi_id}/completado"
                mensaje_completado = f"Taxi {taxi_id} ha completado el servicio para Usuario {user_id}."
                client.publish(topic_completado, mensaje_completado)
                print(f"Taxi {taxi_id} ha completado el servicio para Usuario {user_id}. Tiempo total: {tiempo_total} minutos.")

                # Incrementar el contador de servicios
                servicios_completados += 1

                if servicios_completados >= max_servicios:
                    # Notificar al servidor que la jornada ha finalizado
                    topic_fin_jornada = f"taxis/{taxi_id}/fin_jornada"
                    mensaje_fin_jornada = "Jornada laboral culminada. No puede aceptar más servicios."
                    client.publish(topic_fin_jornada, mensaje_fin_jornada)
                    print(f"Taxi {taxi_id} ha alcanzado el límite de servicios. Notificando fin de jornada.")
                    movimiento_usuario_event.clear()
                    objetivo_usuario = None
                    break  # Detener el hilo de movimiento

                # Retornar inmediatamente a la posición inicial
                print(f"Taxi {taxi_id} regresando a la posición inicial ({x_initial}, {y_initial}).")
                with lock:
                    x, y = x_initial, y_initial  # Retornar a la posición inicial
                publicar_posicion()
                print(f"Taxi {taxi_id} ha regresado a la posición inicial ({x}, {y}). Tiempo total: {tiempo_total} minutos.")

                # Resetear el objetivo
                objetivo_usuario = None
                movimiento_usuario_event.clear()

            else:
                # Movimiento aleatorio según la velocidad
                with lock:
                    # Definir celdas a mover y tiempo de espera basado en la velocidad
                    if velocidad_kmh == 1:
                        celdas_a_mover = 1
                        sleep_time = 60  # 60 segundos reales = 60 minutos simulados
                    elif velocidad_kmh == 2:
                        celdas_a_mover = 1
                        sleep_time = 30  # 30 segundos reales = 30 minutos simulados
                    elif velocidad_kmh == 4:
                        celdas_a_mover = 2
                        sleep_time = 30  # 30 segundos reales = 30 minutos simulados
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
                print(f"Han transcurrido {tiempo_total} minutos. Posición del taxi: ({x}, {y}).")

    # Iniciar el hilo de movimiento
    hilo_movimiento = threading.Thread(target=movimiento_func, daemon=True)
    hilo_movimiento.start()

    # Mantener el hilo principal activo
    try:
        while hilo_movimiento.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        print("Taxi detenido.")
        client.loop_stop()
        client.disconnect()
        sys.exit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Proceso del Taxi")
    parser.add_argument('--taxi_id', type=int, required=True, help='Identificador único del taxi')
    parser.add_argument('--cuadricula_N', type=int, required=True, help='Tamaño N de la cuadrícula')
    parser.add_argument('--cuadricula_M', type=int, required=True, help='Tamaño M de la cuadrícula')
    parser.add_argument('--init_x', type=int, required=True, help='Posición inicial x')
    parser.add_argument('--init_y', type=int, required=True, help='Posición inicial y')
    parser.add_argument('--velocidad', type=int, required=True, choices=[1, 2, 4], help='Velocidad en km/h (1, 2, 4)')
    parser.add_argument('--port', type=int, default=1883, help='Puerto del broker MQTT (default: 1883)')
    args = parser.parse_args()

    broker_addresses = ['invalid.broker.address', 'test.mosquitto.org', 'broker.hivemq.com']  # Lista de brokers MQTT

    taxi_procesos(
        taxi_id=args.taxi_id,
        tam_cuadricula=(args.cuadricula_N, args.cuadricula_M),
        pos_inicial=(args.init_x, args.init_y),
        velocidad_kmh=args.velocidad,
        broker_addresses=broker_addresses,
        broker_port=args.port
    )