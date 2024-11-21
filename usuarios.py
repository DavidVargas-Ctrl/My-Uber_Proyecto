"""
Uso:
    python users.py --num_users <Y> --cuadricula_N <N> --cuadricula_M <M> --coord_file <archivo> --primary_address <address1> --primary_port <port1> --replica_address <address2> --replica_port <port2>

Ejemplo:
    python users.py --num_users 3 --cuadricula_N 50 --cuadricula_M 50 --coord_file coordenadas_Usuarios.txt --primary_address 192.168.0.9 --primary_port 5555 --replica_address 192.168.0.10 --replica_port 5556
"""

import argparse
import threading
import time
import zmq
import sys

def usuario_thread(user_id, pos_x, pos_y, tiempo_min, server_addresses, server_ports):
    """
    Función que representa a un usuario solicitando un taxi.

    Parámetros:
        user_id (int): Identificador del usuario.
        pos_x (int): Posición inicial x.
        pos_y (int): Posición inicial y.
        tiempo_min (int): Tiempo en segundos hasta solicitar un taxi (1 segundo = 1 minuto simulado).
        server_addresses (list): Lista de direcciones de servidores ZeroMQ (primario y réplica).
        server_ports (list): Lista de puertos de servidores ZeroMQ correspondientes.
    """
    print(f"Usuario {user_id} en posición ({pos_x}, {pos_y}) solicitará un taxi en {tiempo_min} minutos simulados.")
    tiempo_inicio = time.time()
    time.sleep(tiempo_min)  # Dormir t segundos representando t minutos simulados (1 segundo real = 1 minuto programa)

    respuesta_obtenida = False

    for address, port in zip(server_addresses, server_ports):
        try:
            context = zmq.Context()
            socket = context.socket(zmq.REQ)
            socket.setsockopt(zmq.RCVTIMEO, 5000)  # Timeout de recepción de 5 segundos
            socket.connect(f"tcp://{address}:{port}")

            mensaje = f"{user_id},{pos_x},{pos_y}"
            socket.send_string(mensaje)
            print(f"Usuario {user_id} ha enviado solicitud al servidor: '{mensaje}' en {address}:{port}")

            try:
                respuesta = socket.recv_string()
                if respuesta.startswith("OK"):
                    taxi_id = respuesta.split()[1]
                    tiempo_total_min = int(time.time() - tiempo_inicio)
                    print(f"Usuario {user_id} ha obtenido un taxi (ID: {taxi_id}) en {tiempo_total_min} minutos simulados.")
                elif respuesta == "NO Taxi disponibles en este momento.":
                    print(f"Usuario {user_id} debe esperar 60 minutos simulados para intentar obtener un taxi.")
                    # Esperar 60 minutos simulados (60 segundos reales)
                    time.sleep(60)
                    # Reintentar la solicitud al mismo servidor
                    socket.send_string(mensaje)
                    print(f"Usuario {user_id} ha reintentado la solicitud al servidor: '{mensaje}' en {address}:{port}")
                    try:
                        respuesta_reintento = socket.recv_string()
                        if respuesta_reintento.startswith("OK"):
                            taxi_id = respuesta_reintento.split()[1]
                            tiempo_total_min = int(time.time() - tiempo_inicio)
                            print(f"Usuario {user_id} ha obtenido un taxi (ID: {taxi_id}) después de esperar {tiempo_total_min} minutos simulados.")
                        else:
                            print(f"Usuario {user_id} no pudo obtener un taxi después de esperar. Respuesta: {respuesta_reintento}")
                    except zmq.Again:
                        print(f"Usuario {user_id} no pudo obtener un taxi después de esperar. Respuesta: No hubo respuesta del servidor.")
                else:
                    print(f"Usuario {user_id} recibió una respuesta desconocida: {respuesta}")
                respuesta_obtenida = True
                socket.close()
                context.term()
                break  # Salir del loop si se obtuvo respuesta
            except zmq.Again:
                print(f"Usuario {user_id} no recibió respuesta del servidor en {address}:{port} dentro del tiempo esperado. Intentando siguiente servidor...")
                socket.close()
                context.term()
                continue  # Intentar el siguiente servidor
        except Exception as e:
            print(f"Usuario {user_id} no pudo conectarse al servidor {address}:{port}. Error: {e}")
            continue  # Intentar el siguiente servidor

    if not respuesta_obtenida:
        print(f"Usuario {user_id} no pudo obtener respuesta de ningún servidor. Se retira del servicio.")

def leer_coordenadas(archivo, num_users):
    """
    Lee las coordenadas iniciales y el tiempo desde un archivo de texto.

    Parámetros:
        archivo (str): Ruta al archivo de coordenadas.
        num_users (int): Número de usuarios.

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

def proceso_generador_users(num_users, N, M, coord_file, primary_address, primary_port, replica_address, replica_port):
    """
    Función principal para generar usuarios.

    Parámetros:
        num_users (int): Número de usuarios a generar.
        N (int): Tamaño N de la cuadrícula.
        M (int): Tamaño M de la cuadrícula.
        coord_file (str): Archivo con coordenadas iniciales.
        primary_address (str): Dirección del servidor principal ZeroMQ.
        primary_port (int): Puerto del servidor principal ZeroMQ.
        replica_address (str): Dirección del servidor réplica ZeroMQ.
        replica_port (int): Puerto del servidor réplica ZeroMQ.
    """
    coordenadas = leer_coordenadas(coord_file, num_users)
    threads = []
    for user_id in range(1, num_users + 1):
        pos_x, pos_y, tiempo_min = coordenadas[user_id - 1]
        thread = threading.Thread(target=usuario_thread, args=(
            user_id, pos_x, pos_y, tiempo_min,
            [primary_address, replica_address],
            [primary_port, replica_port]
        ))
        thread.start()
        threads.append(thread)

    # Esperar a que todos los hilos terminen
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Proceso Generador de Usuarios")
    parser.add_argument('--num_users', type=int, required=True, help='Número de usuarios a generar (Y)')
    parser.add_argument('--cuadricula_N', type=int, required=True, help='Tamaño N de la cuadrícula')
    parser.add_argument('--cuadricula_M', type=int, required=True, help='Tamaño M de la cuadrícula')
    parser.add_argument('--coord_file', type=str, required=True, help='Archivo de coordenadas iniciales')
    parser.add_argument('--primary_address', type=str, required=True, help='Dirección del servidor principal ZeroMQ')
    parser.add_argument('--primary_port', type=int, required=True, help='Puerto del servidor principal ZeroMQ')
    parser.add_argument('--replica_address', type=str, required=True, help='Dirección del servidor réplica ZeroMQ')
    parser.add_argument('--replica_port', type=int, required=True, help='Puerto del servidor réplica ZeroMQ')
    args = parser.parse_args()

    proceso_generador_users(
        num_users=args.num_users,
        N=args.cuadricula_N,
        M=args.cuadricula_M,
        coord_file=args.coord_file,
        primary_address=args.primary_address,
        primary_port=args.primary_port,
        replica_address=args.replica_address,
        replica_port=args.replica_port
    )
