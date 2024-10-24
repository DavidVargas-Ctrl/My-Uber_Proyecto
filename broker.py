import zmq
import threading

def broker_principal():
    context = zmq.Context(1)
    frontend = context.socket(zmq.SUB)
    frontend.bind("tcp://*:5555")  # Conectar taxis al puerto 5555
    frontend.setsockopt(zmq.SUBSCRIBE, b"")

    backend = context.socket(zmq.PUB)
    backend.bind("tcp://*:5556")  # Servidor escucha en el puerto 5556

    try:
        zmq.proxy(frontend, backend)
    except Exception as e:
        print(f"Broker principal falló: {e}")
    finally:
        frontend.close()
        backend.close()
        context.term()

def broker_respaldo():
    context = zmq.Context(1)
    frontend = context.socket(zmq.SUB)
    frontend.bind("tcp://*:5557")  # Conectar taxis al puerto 5557
    frontend.setsockopt(zmq.SUBSCRIBE, b"")

    backend = context.socket(zmq.PUB)
    backend.bind("tcp://*:5558")  # Servidor escucha en el puerto 5558

    try:
        zmq.proxy(frontend, backend)
    except Exception as e:
        print(f"Broker de respaldo falló: {e}")
    finally:
        frontend.close()
        backend.close()
        context.term()

if __name__ == "__main__":
    threading.Thread(target=broker_principal, daemon=True).start()
    threading.Thread(target=broker_respaldo, daemon=True).start()

    while True:
        pass
