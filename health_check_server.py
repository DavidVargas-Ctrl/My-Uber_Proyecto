import zmq
import time
import argparse

def health_server(zmq_host, zmq_port, check_interval):
    """
    HUso:
        python health_check_server.py --zmq_host <direccion_host> --zmq_port <puerto> --check_interval <N>
    Ejemplo:
        python health_check_server.py --zmq_host localhost --zmq_port 5555 --check_interval 5
    """
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.RECONNECT_IVL, 1000)  # Tiempo de reconexión inicial (ms)
    socket.setsockopt(zmq.RECONNECT_IVL_MAX, 5000)  # Máximo tiempo de reconexión (ms)
    socket.connect(f"tcp://{zmq_host}:{zmq_port}")

    print(f"Health Server conectado a tcp://{zmq_host}:{zmq_port}")

    while True:
        try:
            socket.send_string("PING")
            if socket.poll(2000, zmq.POLLIN): 
                respuesta = socket.recv_string()
                if respuesta == "PONG":
                    print(f"Servidor principal está operativo ({time.ctime()}).")
            else:
                print("No se recibió respuesta del servidor principal (timeout).")
        except zmq.ZMQError as e:
            print(f"Error al verificar el estado del servidor: {e}")
            time.sleep(5)
            socket.close()
            socket = context.socket(zmq.REQ)
            socket.setsockopt(zmq.RECONNECT_IVL, 1000)
            socket.setsockopt(zmq.RECONNECT_IVL_MAX, 5000)
            socket.connect(f"tcp://{zmq_host}:{zmq_port}")
        except Exception as e:
            print(f"Excepción inesperada: {e}")
            time.sleep(5)

        # Espera antes del próximo ping
        time.sleep(check_interval)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Health Server para monitorear el servidor principal.")
    parser.add_argument('--zmq_host', type=str, required=True, help="Dirección del servidor ZeroMQ.")
    parser.add_argument('--zmq_port', type=int, required=True, help="Puerto del servidor ZeroMQ.")
    parser.add_argument('--check_interval', type=int, default=5, help="Intervalo en segundos entre cada verificación.")
    args = parser.parse_args()

    health_server(args.zmq_host, args.zmq_port, args.check_interval)
