"""
Uso:
    python users.py --num_users <Y> --cuadricula_N <N> --cuadricula_M <M> --coord_file <archivo> --port <port> --server_address <address>

Ejemplo:
    python usuarios.py --num_usuarios 3 --cuadricula_N 50 --cuadricula_M 50 --coords coordenadas_Usuarios.txt --server 192.168.0.8 --port 5555
"""

import argparse
import threading
import time
import zmq
import sys

def usuario_thread(user_id, pos_x, pos_y, tiempo_sec, server_address, server_port):
    """
    Función que representa a un usuario solicitando un taxi.

    Parámetros:
        user_id (int): Identificador del usuario.
        pos_x (int): Posición inicial x.
        pos_y (int): Posición inicial y.
        tiempo_sec (int): Tiempo en segundos hasta solicitar un taxi.
        server_address (str): Dirección del servidor ZeroMQ.
        server_port (int): Puerto del servidor ZeroMQ.
    """
    print(f"Usuario {user_id} en posición ({pos_x}, {pos_y}) solicitará un taxi en {tiempo_sec} segundos.")
    tiempo_inicio = time.time()
    time.sleep(tiempo_sec)  # Dormir t segundos representando t minutos simulados (1 segundo real = 1 minuto programa)

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(f"tcp://{server_address}:{server_port}")

    mensaje = f"{user_id},{pos_x},{pos_y}"
    start_time = time.time()
    socket.send_string(mensaje)
    print(f"Usuario {user_id} ha enviado solicitud al servidor: '{mensaje}'")

    # Notificar que el usuario está esperando un taxi
    print(f"Usuario {user_id} está esperando un taxi por hasta 60 segundos...")

    # Establecer timeout de 60 segundos reales
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    socks = dict(poller.poll(60000))  # 60000 ms timeout

    if socks.get(socket) == zmq.POLLIN:
        respuesta = socket.recv_string()
        end_time = time.time()
        tiempo_respuesta = end_time - start_time
        tiempo_total_real = end_time - tiempo_inicio  # Tiempo total real desde inicio hasta respuesta
        tiempo_total_simulado = int(tiempo_total_real)  # Convertir a entero para mostrar minutos

        if respuesta.startswith("OK"):
            taxi_id = respuesta.split()[1]
            print(f"Usuario {user_id} ha obtenido un taxi (ID: {taxi_id}) en {tiempo_total_simulado} minutos simulados.")
        else:
            print(f"Usuario {user_id} no pudo obtener un taxi. Respuesta: {respuesta}")
    else:
        # Timeout después de 60 segundos
        print(f"Usuario {user_id} no recibió respuesta del servidor (timeout de 60 segundos). Se retira del servicio.")

    socket.close()
    context.term()

def leer_coordenadas(archivo, num_users):
    """
    Lee las coordenadas iniciales y el tiempo desde un archivo de texto.

    Parámetros:
        archivo (str): Ruta al archivo de coordenadas.
        num_users (int): Número de usuarios.

    Retorna:
        List of tuples: Lista de coordenadas (x, y, tiempo_sec).
    """
    coordenadas = []
    try:
        with open(archivo, 'r') as f:
            for linea in f:
                partes = linea.strip().split()
                if len(partes) != 3:
                    continue
                x, y, tiempo_sec = map(int, partes)
                coordenadas.append((x, y, tiempo_sec))
                if len(coordenadas) == num_users:
                    break
    except Exception as e:
        print(f"Error al leer el archivo de coordenadas: {e}")
        sys.exit(1)

    if len(coordenadas) < num_users:
        print("No hay suficientes coordenadas en el archivo.")
        sys.exit(1)

    return coordenadas

def proceso_generador_users(num_users, N, M, coord_file, server_address, server_port):
    """
    Función principal para generar usuarios.

    Parámetros:
        num_users (int): Número de usuarios a generar.
        N (int): Tamaño N de la cuadrícula.
        M (int): Tamaño M de la cuadrícula.
        coord_file (str): Archivo con coordenadas iniciales.
        server_address (str): Dirección del servidor ZeroMQ.
        server_port (int): Puerto del servidor ZeroMQ.
    """
    coordenadas = leer_coordenadas(coord_file, num_users)
    threads = []
    for user_id in range(1, num_users + 1):
        pos_x, pos_y, tiempo_sec = coordenadas[user_id - 1]
        thread = threading.Thread(target=usuario_thread, args=(
            user_id, pos_x, pos_y, tiempo_sec, server_address, server_port))
        thread.start()
        threads.append(thread)

    # Esperar a que todos los hilos terminen
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Proceso Generador de Usuarios")
    parser.add_argument('--num_usuarios', type=int, required=True, help='Número de usuarios a generar (Y)')
    parser.add_argument('--cuadricula_N', type=int, required=True, help='Tamaño N de la cuadrícula')
    parser.add_argument('--cuadricula_M', type=int, required=True, help='Tamaño M de la cuadrícula')
    parser.add_argument('--coords', type=str, required=True, help='Archivo de coordenadas iniciales')
    parser.add_argument('--server_address', type=str, default='localhost', help='Dirección del servidor ZeroMQ (default: localhost)')
    parser.add_argument('--port', type=int, default=5555, help='Puerto del servidor ZeroMQ (default: 5555)')
    args = parser.parse_args()

    proceso_generador_users(
        num_users=args.num_usuarios,
        N=args.cuadricula_N,
        M=args.cuadricula_M,
        coord_file=args.coords,
        server_address=args.server_address,
        server_port=args.port
    )
