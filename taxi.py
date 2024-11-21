"""
Uso:
    python taxi.py --taxi_id <id> --cuadricula_N <N> --cuadricula_M <M> --init_x <x> --init_y <y> --velocidad <velocidad> --broker_address <address> --broker_port <port>

Ejemplos:
    python taxi.py --taxi_id 1 --cuadricula_N 50 --cuadricula_M 50 --init_x 5 --init_y 5 --velocidad 2 --broker_address test.mosquitto.org
    python taxi.py --taxi_id 3 --cuadricula_N 50 --cuadricula_M 50 --init_x 6 --init_y 7 --velocidad 4 --broker_address broker.emqx.io
"""

import time
import threading
import random
import argparse
import sys
import paho.mqtt.client as mqtt

def taxi_procesos(taxi_id, tam_cuadricula, pos_inicial, velocidad_kmh, broker_address, broker_port):
    """
    Función principal para el proceso del taxi.

    Parámetros:
        taxi_id (int): Identificador único del taxi.
        tam_cuadricula (tuple): Dimensiones de la cuadrícula (N, M).
        pos_inicial (tuple): Posición inicial (x, y) del taxi.
        velocidad_kmh (int): Velocidad de movimiento en km/h (1, 2 o 4).
        broker_address (str): Dirección del broker MQTT.
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

    # Variables para gestionar la conexión MQTT
    client = None
    conectado = False

    # Configuración del tópico al que se está publicando posiciones
    topic_posicion = f"taxis/{taxi_id}/posicion"

    # Variables para gestionar el movimiento hacia usuarios
    movimiento_usuario_event = threading.Event()
    objetivo_usuario = None  # (user_id, user_x, user_y)

    def on_connect(client, userdata, flags, rc):
        nonlocal conectado
        if rc == 0:
            conectado = True
            topic_servicio = f"taxis/{taxi_id}/servicio"
            client.subscribe(topic_servicio)
            print(f"Taxi {taxi_id} conectado al broker MQTT y suscrito a {topic_servicio}")
        else:
            print(f"Taxi {taxi_id} error al conectar al broker MQTT, código de retorno {rc}")
            conectado = False

    def on_disconnect(client, userdata, rc):
        nonlocal conectado
        print(f"Taxi {taxi_id} desconectado del broker MQTT con código de retorno {rc}")
        conectado = False
        # Intentar reconectar al broker
        reconectar_broker()

    def on_message(client, userdata, msg):
        nonlocal servicios_completados, objetivo_usuario, tiempo_total
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

    def conectar_broker():
        """
        Conectar al broker MQTT único.
        """
        nonlocal client, conectado
        try:
            client = mqtt.Client(client_id=f"Taxi_{taxi_id}")
            client.on_connect = on_connect
            client.on_disconnect = on_disconnect
            client.on_message = on_message
            client.connect(broker_address, broker_port, 60)
            client.loop_start()
            print(f"Taxi {taxi_id} intentando conectarse al broker MQTT en {broker_address}:{broker_port}")
        except Exception as e:
            print(f"Taxi {taxi_id} no pudo conectarse al broker MQTT en {broker_address}:{broker_port}. Error: {e}")
            sys.exit(1)

    def reconectar_broker():
        """
        Intentar reconectar al broker MQTT.
        """
        nonlocal client, conectado
        print(f"Taxi {taxi_id} intentando reconectarse al broker MQTT en {broker_address}:{broker_port}")
        try:
            client.reconnect()
            conectado = True
        except Exception as e:
            print(f"Taxi {taxi_id} fallo al intentar reconectarse: {e}")
            time.sleep(5)  # Esperar 5 segundos antes de reintentar
            reconectar_broker()

    def publicar_posicion_periodica():
        """ Publica la posición actual del taxi al broker MQTT cada intervalo basado en la velocidad. """
        while True:
            publicar_posicion()
            time.sleep(1)  # Intervalo de publicación; ajusta según sea necesario

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

    # Iniciar la conexión al broker MQTT
    conectar_broker()

    # Iniciar el hilo de movimiento
    hilo_movimiento = threading.Thread(target=movimiento_func, daemon=True)
    hilo_movimiento.start()

    # Mantener el hilo principal activo
    try:
        while hilo_movimiento.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        print("Taxi detenido.")
        if client is not None:
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
    parser.add_argument('--broker_address', type=str, required=True, help='Dirección del broker MQTT único')
    parser.add_argument('--broker_port', type=int, default=1883, help='Puerto del broker MQTT (default: 1883)')
    args = parser.parse_args()

    taxi_procesos(
        taxi_id=args.taxi_id,
        tam_cuadricula=(args.cuadricula_N, args.cuadricula_M),
        pos_inicial=(args.init_x, args.init_y),
        velocidad_kmh=args.velocidad,
        broker_address=args.broker_address,
        broker_port=args.broker_port
    )
