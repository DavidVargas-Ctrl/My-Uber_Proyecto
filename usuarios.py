# users.py

"""
Uso:
    python users.py --num_users <Y> --cuadricula_N <N> --cuadricula_M <M> --coord_file <archivo> --port <port> --server_address <address>

Ejemplo:
    python usuarios.py --num_usuarios 1 --cuadricula_N 50 --cuadricula_M 50 --coords coordenadas_Usuarios.txt --server 192.168.0.5
"""

import argparse
import threading
import time
import random
import zmq
import sys

def usuario_thread(user_id, pos_x, pos_y, tiempo_min, server_address, server_port):
    """
    Funcion que representa a un usuario solicitando un taxi.

    Parametros:
        user_id (int): Identificador del usuario.
        pos_x (int): Posicion inicial x.
        pos_y (int): Posicion inicial y.
        tiempo_min (int): Tiempo en minutos hasta solicitar un taxi.
        server_address (str): Direccion del servidor ZeroMQ.
        server_port (int): Puerto del servidor ZeroMQ.
    """
    print(f"Usuario {user_id} en posicion ({pos_x}, {pos_y}) solicitara un taxi en {tiempo_min} segundos.")
    tiempo_inicio = time.time()
    time.sleep(tiempo_min)  # Dormir t segundos representando t minutos

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(f"tcp://{server_address}:{server_port}")

    mensaje = f"{user_id},{pos_x},{pos_y}"
    start_time = time.time()
    socket.send_string(mensaje)
    print(f"Usuario {user_id} ha enviado solicitud al servidor: '{mensaje}'")

    # Establecer timeout de 10 segundos
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    socks = dict(poller.poll(10000))  # 10000 ms timeout

    if socks.get(socket) == zmq.POLLIN:
        respuesta = socket.recv_string()
        end_time = time.time()
        tiempo_respuesta = end_time - start_time
        tiempo_total = end_time - tiempo_inicio  # Incluir tiempo de espera antes de solicitar
        tiempo_programa = tiempo_total  # 1 segundo real = 1 minuto programa
        if respuesta.startswith("OK"):
            taxi_id = respuesta.split()[1]
            print(f"Usuario {user_id} ha obtenido un taxi (ID: {taxi_id}) en {tiempo_programa:.1f} minutos.")
        else:
            print(f"Usuario {user_id} no pudo obtener un taxi. Respuesta: {respuesta}")
    else:
        print(f"Usuario {user_id} no recibió respuesta del servidor (timeout).")

    socket.close()
    context.term()

def leer_coordenadas(archivo, num_users):
    """
    Lee las coordenadas iniciales desde un archivo de texto.

    Parametros:
        archivo (str): Ruta al archivo de coordenadas.
        num_users (int): Numero de usuarios.

    Retorna:
        List of tuples: Lista de coordenadas (x, y).
    """
    coordenadas = []
    try:
        with open(archivo, 'r') as f:
            for linea in f:
                partes = linea.strip().split()
                if len(partes) != 2:
                    continue
                x, y = map(int, partes)
                coordenadas.append((x, y))
                if len(coordenadas) == num_users:
                    break
    except Exception as e:
        print(f"Error al leer el archivo de coordenadas: {e}")
        sys.exit(1)

    if len(coordenadas) < num_users:
        print("No hay suficientes coordenadas en el archivo.")
        sys.exit(1)

    return coordenadas

def proceso_generador_users(num_users, N, M, coord_file, broker_address, broker_port):
    """
    Funcion principal para generar usuarios.

    Parametros:
        num_users (int): Numero de usuarios a generar.
        N (int): Tamaño N de la cuadrícula.
        M (int): Tamaño M de la cuadrícula.
        coord_file (str): Archivo con coordenadas iniciales.
        broker_address (str): Direccion del servidor ZeroMQ.
        broker_port (int): Puerto del servidor ZeroMQ.
    """
    coordenadas = leer_coordenadas(coord_file, num_users)
    threads = []
    for user_id in range(1, num_users + 1):
        pos_x, pos_y = coordenadas[user_id - 1]
        # Asignar un tiempo t aleatorio entre 1 y 10 minutos (segundos)
        tiempo_min = random.randint(1, 10)
        thread = threading.Thread(target=usuario_thread, args=(
            user_id, pos_x, pos_y, tiempo_min, broker_address, broker_port))
        thread.start()
        threads.append(thread)

    # Esperar a que todos los hilos terminen
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Proceso Generador de Usuarios")
    parser.add_argument('--num_usuarios', type=int, required=True, help='Numero de usuarios a generar (Y)')
    parser.add_argument('--cuadricula_N', type=int, required=True, help='Tamaño N de la cuadrícula')
    parser.add_argument('--cuadricula_M', type=int, required=True, help='Tamaño M de la cuadrícula')
    parser.add_argument('--coords', type=str, required=True, help='Archivo de coordenadas iniciales')
    parser.add_argument('--server', type=str, default='localhost', help='Dirección del servidor ZeroMQ (default: localhost)')
    args = parser.parse_args()

    proceso_generador_users(
        num_users=args.num_users,
        N=args.cuadricula_N,
        M=args.cuadricula_M,
        coord_file=args.coords,
        broker_address=args.server_address,
        broker_port=5555
    )
