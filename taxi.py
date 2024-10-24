"""
Uso:
    python taxi.py --taxi_id <id> --init_x <x> --init_y <y> --velocidad <velocidad> --broker <broker_address>

Ejemplos:
    python taxi.py --taxi_id 1 --cuadricula_N 50 --cuadricula_M 50 --init_x 5 --init_y 5 --velocidad 2 --port 1884
    python taxi.py --taxi_id 3 --cuadricula_N 50 --cuadricula_M 50 --init_x 6 --init_y 7 --velocidad 4 --port 1883
"""

import time
import threading
import random
import argparse
import socket
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
    x_inicial, y_inicial = pos_inicial  # Guardar la posición inicial del taxi
    x, y = pos_inicial  # Posición actual del taxi
    minutos_transcurridos = 0

    # Independendecia entre hilos
    lock = threading.Lock()

    # Configuración del cliente MQTT
    client = mqtt.Client(client_id=f"Taxi_{taxi_id}")
    client.connect(broker_address, broker_port, 60)
    client.loop_start()

    # Configuracion del topic al que se esta subscribiendo
    topic_posicion = f"taxis/{taxi_id}/posicion"

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

            # Moverse celdas_a_mover veces en direcciones aleatorias
            with lock:
                for _ in range(celdas_a_mover):
                    direccion = random.choice(['N', 'S', 'E', 'O'])
                    nuevo_x, nuevo_y = x, y
                    if direccion == 'N' and y < N:
                        nuevo_y += 1
                    elif direccion == 'S' and y > 0:
                        nuevo_y -= 1
                    elif direccion == 'E' and x < M:
                        nuevo_x += 1
                    elif direccion == 'O' and x > 0:
                        nuevo_x -= 1
                    else:
                        print(f"Taxi {taxi_id} ha alcanzado el límite en dirección {direccion}.")
                        continue  # No moverse si se alcanza el límite

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

def get_local_ip():
    """
    Función auxiliar para obtener la dirección IP local de la máquina.

    Retorna:
        str: Dirección IP local.
    """
    ipLocal = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        ipLocal.connect(('10.255.255.255', 1))
        IP = ipLocal.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        ipLocal.close()
    return IP

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
        broker_address='10.43.100.114',
        broker_port=args.port
    )
