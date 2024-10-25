"""
Uso:
    python taxi.py --taxi_id <id> --init_x <x> --init_y <y> --velocidad <velocidad> --broker <broker_address>

Ejemplos:
    python taxi.py --taxi_id 1 --cuadricula_N 50 --cuadricula_M 50 --init_x 5 --init_y 5 --velocidad 2 --port 1883
    python taxi.py --taxi_id 1 --cuadricula_N 50 --cuadricula_M 50 --init_x 5 --init_y 5 --velocidad 2 --brokers 10.43.100.114:1883,10.43.100.115:1884

"""

import time
import threading
import random
import argparse
import sys
import paho.mqtt.client as mqtt

def taxi_procesos(taxi_id, tam_cuadricula, pos_inicial, velocidad_kmh, brokers):
    """
    Función principal para el proceso del taxi.

    Parámetros:
        taxi_id (int): Identificador único del taxi.
        tam_cuadricula (tuple): Dimensiones de la cuadrícula (N, M).
        pos_inicial (tuple): Posición inicial (x, y) del taxi.
        velocidad_kmh (int): Velocidad de movimiento en km/h (1, 2 o 4).
        brokers (list): Lista de tuplas con dirección y puerto de brokers MQTT.
    """

    N, M = tam_cuadricula
    x_inicial, y_inicial = pos_inicial  # Guardar la posición inicial del taxi
    x, y = pos_inicial  # Posición actual del taxi
    minutos_transcurridos = 0
    servicios_realizados = 0

    # Independendecia entre hilos
    lock = threading.Lock()

    clients = []  # Lista para almacenar los clientes MQTT

    # Definir el tópico de posición
    topic_posicion = f"taxis/{taxi_id}/posicion"

    def conexion(client, info, bn, rc):
        if rc == 0:
            print(f"Taxi {taxi_id} conectado al broker MQTT {client.broker_address}:{client.broker_port} exitosamente.")

        else:
            print(
                f"Error al conectar al broker MQTT {client.broker_address}:{client.broker_port}, código de retorno {rc}")
            sys.exit(1)

    # Crear y configurar un cliente MQTT para cada broker
    for idx, (broker_direccion, broker_puerto) in enumerate(brokers):
        client_id = f"Taxi_{taxi_id}_{idx}"
        client = mqtt.Client(client_id=client_id)
        client.broker_address = broker_direccion
        client.broker_port = broker_puerto

        # Asignar funciones de callback
        client.on_connect = conexion

        try:
            client.connect(broker_direccion, broker_puerto, 60)
        except Exception as e:
            print(f"No se pudo conectar al broker MQTT {broker_direccion}:{broker_puerto}: {e}")
            sys.exit(1)

        client.loop_start()  # Iniciar el loop para manejar la comunicación

        clients.append(client)

    def publicar_posicion(x, y):
        """ Publica la posición actual del taxi al broker MQTT. """
        mensaje = f"{x} {y}"
        client.publish(topic_posicion, mensaje)

    # Enviar la posición inicial
    print(f"Taxi {taxi_id} se ha registrado exitosamente en el servidor.")
    print(f"Posición inicial del Taxi {taxi_id}: ({x_inicial}, {y_inicial})")
    publicar_posicion(x_inicial, y_inicial)

    # Tiempo establecido por cada hilo
    evento_tiempo = threading.Event()


    def movimiento_taxi():
        nonlocal x, y, minutos_transcurridos
        tiempo_espera = False
        ultimo_pos = None
        direcciones_opuestas = {
            'N': 'S',
            'S': 'N',
            'E': 'O',
            'O': 'E'
        }

        # Definir intervalo y número de celdas en función de la velocidad
        if velocidad_kmh == 4:
            intervalo = 30  # 30 segundos
            celdas_a_mover = 2  # Mover 2 celdas cada 30 segundos
        elif velocidad_kmh == 2:
            intervalo = 30  # 30 segundos
            celdas_a_mover = 1  # Mover 1 celda cada 30 segundos
        elif velocidad_kmh == 1:
            # Taxi permanece en posición inicial durante los primeros 30 segundos
            intervalo = 30  # 30 segundos sin moverse
            celdas_a_mover = 0  # No moverse inicialmente

        while True:
            time.sleep(intervalo)  # Esperar el tiempo correspondiente
            minutos_transcurridos += 30  # Actualizar minutos transcurridos

            if velocidad_kmh == 1 and not tiempo_espera:
                tiempo_espera = True
                celdas_a_mover = 1


            with lock:
                for _ in range(celdas_a_mover):
                    # Definir direcciones permitidas evitando la dirección opuesta
                    if ultimo_pos:
                        direcciones_permitidas = ['N', 'S', 'E', 'O']
                        # Excluir la dirección opuesta a la última
                        direcciones_permitidas.remove(direcciones_opuestas[ultimo_pos])
                    else:
                        direcciones_permitidas = ['N', 'S', 'E', 'O']

                    # Si el taxi no puede moverse en ninguna dirección salta el movimiento
                    if not direcciones_permitidas:
                        continue

                    # Elegir una dirección aleatoria de las permitidas
                    nueva_pos = random.choice(direcciones_permitidas)

                    # Actualizar la última dirección
                    ultimo_pos = nueva_pos

                    nuevo_x, nuevo_y = x, y
                    if nueva_pos == 'N' and y < N:
                        nuevo_y += 1
                    elif nueva_pos == 'S' and y > 0:
                        nuevo_y -= 1
                    elif nueva_pos == 'E' and x < M:
                        nuevo_x += 1
                    elif nueva_pos == 'O' and x > 0:
                        nuevo_x -= 1
                    else:
                        print(f"Taxi {taxi_id} ha alcanzado el límite en dirección {nueva_pos}.")
                        continue  # No moverse si se alcanza el límite

                    # Solo actualizar si la nueva posición es diferente
                    if (nuevo_x, nuevo_y) != (x, y):
                        x, y = nuevo_x, nuevo_y


                publicar_posicion(x, y)

            evento_tiempo.set()

    def mostrar_informacion_tiempo():
        while True:
            evento_tiempo.wait()  # Esperar hasta que el evento sea activado
            with lock:
                print(f"Han transcurrido {minutos_transcurridos} minutos.")
                print(f"Su posición actual es: ({x}, {y})")  # Imprimir la posición actual
            evento_tiempo.clear()

    threading.Thread(target=mostrar_informacion_tiempo, daemon=True).start()
    threading.Thread(target=movimiento_taxi, daemon=True).start()

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
    parser.add_argument('--brokers', type=str, required=True,
                        help='Lista de brokers en formato ip:port separados por comas')
    args = parser.parse_args()


    brokers = []
    for broker in args.brokers.split(','):
        try:
            ip, port = broker.split(':')
            brokers.append((ip.strip(), int(port.strip())))
        except ValueError:
            print(f"Formato de broker inválido: {broker}. Debe ser ip:port")
            sys.exit(1)

    taxi_procesos(
        taxi_id=args.taxi_id,
        tam_cuadricula=(args.cuadricula_N, args.cuadricula_M),
        pos_inicial=(args.init_x, args.init_y),
        velocidad_kmh=args.velocidad,
        brokers=brokers
    )
