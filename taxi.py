"""
Uso:
    python taxi.py --taxi_id <id> --cuadricula_N <N> --cuadricula_M <M> --init_x <x> --init_y <y> --velocidad <velocidad> --port <port>

Ejemplos:
    python taxi.py --taxi_id 1 --cuadricula_N 50 --cuadricula_M 50 --init_x 5 --init_y 5 --velocidad 2 --port 1883
    python taxi.py --taxi_id 3 --cuadricula_N 50 --cuadricula_M 50 --init_x 6 --init_y 7 --velocidad 4 --port 1883
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
    minutos_transcurridos = 0

    # Independencia entre hilos
    lock = threading.Lock()

    # Configuración del cliente MQTT
    client = mqtt.Client(client_id=f"Taxi_{taxi_id}")
    client.connect(broker_address, broker_port, 60)
    client.loop_start()

    # Configuración del tópico al que se está publicando posiciones
    topic_posicion = f"taxis/{taxi_id}/posicion"

    # Variables para gestionar el movimiento hacia usuarios
    movimiento_usuario_event = threading.Event()
    objetivo_usuario = None  # (user_id, user_x, user_y)

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            topic_servicio = f"taxis/{taxi_id}/servicio"
            client.subscribe(topic_servicio)
            print(f"Taxi {taxi_id} conectado al broker MQTT y suscrito a {topic_servicio}")
        else:
            print(f"Taxi {taxi_id} error al conectar al broker MQTT, código de retorno {rc}")
            sys.exit(1)

    def on_message(client, userdata, msg):
        try:
            topic_parts = msg.topic.split('/')
            if len(topic_parts) != 3 or topic_parts[0] != 'taxis' or topic_parts[2] != 'servicio':
                return

            # Mensaje de servicio: "Usuario {user_id} en (x, y)"
            mensaje = msg.payload.decode()
            partes = mensaje.split()
            if len(partes) < 5:
                return
            user_id = partes[1]
            user_x = int(partes[3].strip('(').strip(','))
            user_y = int(partes[4].strip(')'))

            # Simular mover al usuario
            mover_al_usuario(user_id, user_x, user_y)

        except Exception as e:
            print(f"Taxi {taxi_id} error al procesar mensaje de servicio: {e}")

    def mover_al_usuario(user_id, user_x, user_y):
        """
        Simular el movimiento del taxi hacia la posición del usuario.

        Parámetros:
            user_id (str): Identificador del usuario.
            user_x (int): Posición x del usuario.
            user_y (int): Posición y del usuario.
        """
        nonlocal x, y, minutos_transcurridos, objetivo_usuario
        with lock:
            if objetivo_usuario is not None:
                print(f"Taxi {taxi_id} ya está atendiendo una solicitud. Ignorando nueva solicitud de Usuario {user_id}.")
                return  # Ya está atendiendo a un usuario

            objetivo_usuario = (user_id, user_x, user_y)
            print(f"Taxi {taxi_id} moviéndose hacia el Usuario {user_id} en ({user_x}, {user_y})")
            movimiento_usuario_event.set()

    def publicar_posicion():
        """ Publica la posición actual del taxi al broker MQTT. """
        mensaje = f"{x} {y}"
        client.publish(topic_posicion, mensaje)
        print(f"Taxi {taxi_id} publicó posición: ({x}, {y})")

    # Asignar callbacks
    client.on_connect = on_connect
    client.on_message = on_message

    print(f"Taxi {taxi_id} se ha registrado exitosamente en el servidor.")
    print(f"Posición inicial del Taxi {taxi_id}: ({x}, {y})")
    publicar_posicion()

    def movimiento():
        nonlocal x, y, minutos_transcurridos, objetivo_usuario

        while True:
            if movimiento_usuario_event.is_set():
                # Mover hacia el usuario
                user_id, user_x, user_y = objetivo_usuario
                print(f"Taxi {taxi_id} se dirige hacia el usuario {user_id} en ({user_x}, {user_y})")
                while (x, y) != (user_x, user_y):
                    with lock:
                        if x < user_x:
                            x += 1
                        elif x > user_x:
                            x -= 1
                        if y < user_y:
                            y += 1
                        elif y > user_y:
                            y -= 1
                        # Simular tiempo de movimiento por celda
                        time.sleep(30 / velocidad_kmh)  # 1 segundo real = 1 minuto programa

                    # No publicar cada movimiento, solo al llegar
                # Llegó al usuario
                print(f"Taxi {taxi_id} ha llegado al Usuario {user_id} en ({x}, {y})")
                # Simular tiempo de servicio
                tiempo_servicio = 30  # 30 segundos reales = 30 minutos programa
                print(f"Taxi {taxi_id} está realizando el servicio al Usuario {user_id} por {tiempo_servicio} segundos.")
                time.sleep(tiempo_servicio)  # 1 segundo real = 1 minuto programa

                # Notificar al servidor que el servicio ha sido completado
                topic_completado = f"taxis/{taxi_id}/completado"
                mensaje_completado = f"Taxi {taxi_id} ha completado el servicio para Usuario {user_id}."
                client.publish(topic_completado, mensaje_completado)
                print(f"Taxi {taxi_id} ha completado el servicio para Usuario {user_id}.")

                # Resetear el objetivo
                with lock:
                    objetivo_usuario = None
                movimiento_usuario_event.clear()
            else:
                # Movimiento aleatorio cada 30 segundos
                with lock:
                    celdas_a_mover = {1: 0, 2: 1, 4: 2}.get(velocidad_kmh, 1)
                    for _ in range(celdas_a_mover):
                        dx, dy = random.choice([(0, 1), (0, -1), (1, 0), (-1, 0)])  # Movimiento aleatorio
                        nuevo_x, nuevo_y = x + dx, y + dy

                        # Validar límites de la cuadrícula
                        if 0 <= nuevo_x < N and 0 <= nuevo_y < M:
                            x, y = nuevo_x, nuevo_y

                # Esperar el intervalo antes de publicar
                time.sleep(30)  # 30 segundos reales = 30 minutos programa
                minutos_transcurridos += 30

                # Publicar posición solo después de completar el intervalo
                publicar_posicion()
                print(f"Han transcurrido {minutos_transcurridos} minutos. Posición del taxi: ({x}, {y})")

    # Iniciar el hilo de movimiento
    threading.Thread(target=movimiento, daemon=True).start()

    # Mantener el hilo principal activo
    try:
        while True:
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

    taxi_procesos(
        taxi_id=args.taxi_id,
        tam_cuadricula=(args.cuadricula_N, args.cuadricula_M),
        pos_inicial=(args.init_x, args.init_y),
        velocidad_kmh=args.velocidad,
        broker_address='test.mosquitto.org',  # Asegúrate de que coincida con el servidor
        broker_port=args.port
    )
