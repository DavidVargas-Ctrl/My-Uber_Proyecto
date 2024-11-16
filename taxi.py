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

    def publicar_posicion():
        """ Publica la posición actual del taxi al broker MQTT. """
        mensaje = f"{x} {y}"
        client.publish(topic_posicion, mensaje)
        print(f"Taxi {taxi_id} publicó posición: ({x}, {y})")

    print(f"Taxi {taxi_id} se ha registrado exitosamente en el servidor.")
    print(f"Posición inicial del Taxi {taxi_id}: ({x}, {y})")
    publicar_posicion()

    def movimiento_random():
        nonlocal x, y, minutos_transcurridos

        # Mapear velocidad en celdas por intervalo (cada 30 segundos)
        celdas_por_intervalo = {
            1: 0,  # No se mueve en los primeros 30 segundos
            2: 1,  # Mueve 1 celda cada 30 segundos
            4: 2,  # Mueve 2 celdas cada 30 segundos
        }
        movimiento = celdas_por_intervalo[velocidad_kmh]

        while True:
            time.sleep(30)  # Esperar 30 segundos reales
            minutos_transcurridos += 30  # Incrementar minutos simulados

            with lock:
                for _ in range(movimiento):
                    dx, dy = random.choice([(0, 1), (0, -1), (1, 0), (-1, 0)])  # Movimiento aleatorio
                    nuevo_x, nuevo_y = x + dx, y + dy

                    # Validar límites de la cuadrícula
                    if 0 <= nuevo_x < N and 0 <= nuevo_y < M:
                        x, y = nuevo_x, nuevo_y

                # Publicar posición solo después de completar el intervalo de 30 minutos simulados
                publicar_posicion()
                print(f"Han transcurrido {minutos_transcurridos} minutos. Posición del taxi: ({x}, {y})")

    # Iniciar el hilo de movimiento
    threading.Thread(target=movimiento_random, daemon=True).start()

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
        broker_address='test.mosquitto.org',
        broker_port=args.port
    )
