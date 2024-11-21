"""
Uso:
    python users.py --num_usuarios <Y> --cuadricula_N <N> --cuadricula_M <M> --coords <archivo> --server_address <address>

Ejemplo:
    python3 usuarios.py --num_usuarios 3 --cuadricula_N 50 --cuadricula_M 50 --coords coordenadas_Usuarios.txt --server_address 192.168.0.9
    python usuarios.py --num_usuarios 1 --cuadricula_N 50 --cuadricula_M 50 --coords coordenadas_Usuarios.txt --server_address 192.168.0.9
"""

import argparse
import threading
import time
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
    print(f"Usuario {user_id} en posicion ({pos_x}, {pos_y}) solicitara un taxi en {tiempo_min} minutos.")
    tiempo_inicio = time.time()
    time.sleep(tiempo_min)

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(f"tcp://{server_address}:{server_port}")

    mensaje = f"{user_id},{pos_x},{pos_y}"
    socket.send_string(mensaje)
    print(f"Usuario {user_id} ha enviado solicitud al servidor: '{mensaje}'")

    # Esperar la respuesta del servidor
    try:
        # Esperar 2 segundos antes de intentar recibir la respuesta
        time.sleep(2)
        respuesta = socket.recv_string(flags=zmq.NOBLOCK)

        if respuesta.startswith("OK"):
            taxi_id = respuesta.split()[1]
            tiempo_total_min = int(time.time() - tiempo_inicio)
            print(f"Usuario {user_id} ha obtenido un taxi (ID: {taxi_id}) en {tiempo_total_min} minutos.")
        elif respuesta == "NO Taxi disponibles en este momento.":
            print(f"Usuario {user_id} debe esperar 60 minutos para intentar obtener un taxi.")
            # Esperar 60 minutos antes de reintentar
            time.sleep(60)
            # Reintentar la solicitud
            socket.send_string(mensaje)
            print(f"Usuario {user_id} ha reintentado la solicitud al servidor: '{mensaje}'")
            try:
                respuesta_reintento = socket.recv_string()
                if respuesta_reintento.startswith("OK"):
                    taxi_id = respuesta_reintento.split()[1]
                    tiempo_total_min = int(time.time() - tiempo_inicio)
                    print(f"Usuario {user_id} ha obtenido un taxi (ID: {taxi_id}) después de esperar {tiempo_total_min} minutos.")
                else:
                    print(f"Usuario {user_id} no pudo obtener un taxi después de esperar. Respuesta: {respuesta_reintento}")
            except zmq.Again:
                print(f"Usuario {user_id} no pudo obtener un taxi después de esperar. Respuesta: No hubo respuesta del servidor.")
        else:
            print(f"Usuario {user_id} recibió una respuesta desconocida: {respuesta}")
    except zmq.Again:
        # No se recibió respuesta inmediatamente, esperar hasta 60 minutos
        print(f"Usuario {user_id} no recibió respuesta inmediata. Esperando hasta 60 minutos...")
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        socks = dict(poller.poll(60000))  # 60000 ms timeout (60 segundos reales = 60 minutos simulados)

        if socks.get(socket) == zmq.POLLIN:
            respuesta = socket.recv_string()
            if respuesta.startswith("OK"):
                taxi_id = respuesta.split()[1]
                tiempo_total_min = int(time.time() - tiempo_inicio)
                print(f"Usuario {user_id} ha obtenido un taxi (ID: {taxi_id}) en {tiempo_total_min} minutos.")
            elif respuesta == "NO Taxi disponibles en este momento.":
                print(f"Usuario {user_id} debe esperar 60 minutos para intentar obtener un taxi.")
                # Esperar 60 minutos antes de reintentar (60 segundos reales)
                time.sleep(60)
                # Reintentar la solicitud
                socket.send_string(mensaje)
                print(f"Usuario {user_id} ha reintentado la solicitud al servidor: '{mensaje}'")
                try:
                    respuesta_reintento = socket.recv_string()
                    if respuesta_reintento.startswith("OK"):
                        taxi_id = respuesta_reintento.split()[1]
                        tiempo_total_min = int(time.time() - tiempo_inicio)
                        print(f"Usuario {user_id} ha obtenido un taxi (ID: {taxi_id}) después de esperar {tiempo_total_min} minutos.")
                    else:
                        print(f"Usuario {user_id} no pudo obtener un taxi después de esperar. Respuesta: {respuesta_reintento}")
                except zmq.Again:
                    print(f"Usuario {user_id} no pudo obtener un taxi después de esperar. Respuesta: No hubo respuesta del servidor.")
            else:
                print(f"Usuario {user_id} recibió una respuesta desconocida: {respuesta}")
        else:
            print(f"Usuario {user_id} no recibió respuesta del servidor (timeout de 60 minutos). Se retira del servicio.")

    socket.close()
    context.term()

def leer_coordenadas(archivo, num_users):
    """
    Lee las coordenadas iniciales y el tiempo desde un archivo de texto.

    Parametros:
        archivo (str): Ruta al archivo de coordenadas.
        num_users (int): Numero de usuarios.

    Retorna:
        List of tuples: Lista de coordenadas (x, y, tiempo_min).
    """
    coordenadas = []
    try:
        with open(archivo, 'r') as f:
            for linea in f:
                partes = linea.strip().split()
                if len(partes) != 3:
                    continue
                x, y, tiempo_min = map(int, partes)
                coordenadas.append((x, y, tiempo_min))
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
    Funcion principal para generar usuarios.

    Parametros:
        num_users (int): Numero de usuarios a generar.
        N (int): Tamaño N de la cuadrícula.
        M (int): Tamaño M de la cuadrícula.
        coord_file (str): Archivo con coordenadas iniciales.
        server_address (str): Direccion del servidor ZeroMQ.
        server_port (int): Puerto del servidor ZeroMQ.
    """
    coordenadas = leer_coordenadas(coord_file, num_users)
    threads = []
    for user_id in range(1, num_users + 1):
        pos_x, pos_y, tiempo_min = coordenadas[user_id - 1]
        thread = threading.Thread(target=usuario_thread, args=(
            user_id, pos_x, pos_y, tiempo_min, server_address, server_port))
        thread.start()
        threads.append(thread)

    # Esperar a que todos los hilos terminen
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Proceso Generador de Usuarios")
    parser.add_argument('--num_usuarios', type=int, required=True, help='Numero de usuarios a generar')
    parser.add_argument('--cuadricula_N', type=int, required=True, help='Tamaño N de la cuadrícula')
    parser.add_argument('--cuadricula_M', type=int, required=True, help='Tamaño M de la cuadrícula')
    parser.add_argument('--coords', type=str, required=True, help='Archivo de coordenadas iniciales')
    parser.add_argument('--server_address', type=str, default='localhost', help='Direccion del servidor ZeroMQ')
    args = parser.parse_args()

    proceso_generador_users(
        num_users=args.num_usuarios,
        N=args.cuadricula_N,
        M=args.cuadricula_M,
        coord_file=args.coords,
        server_address=args.server_address,
        server_port=5555
    )
