"""
python3 health_check.py
"""


import time
import threading
import paho.mqtt.client as mqtt
from datetime import datetime


MQTT_BROKERS = [
    {'address': '10.43.101.111', 'port': 1883},
    {'address': '10.43.100.114', 'port': 1883}
]

# Intervalo en segundos entre cada verificación de salud
CHECK_INTERVAL = 10

# Archivo de reporte
REPORT_FILE = "health_report.txt"

# Lock para sincronizar el acceso al archivo de reporte
report_lock = threading.Lock()


def log_health_status(broker, status, message=""):
    """
    Registra el estado de salud del broker en el archivo de reporte.

    Parámetros:
        broker (dict): Diccionario con 'address' y 'port' del broker.
        status (str): Estado del broker ('CONNECTED' o 'DISCONNECTED').
        message (str): Mensaje adicional opcional.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] Broker {broker['address']}:{broker['port']} - {status}"
    if message:
        log_entry += f" - {message}"
    log_entry += "\n"

    with report_lock:
        with open(REPORT_FILE, 'a') as f:
            f.write(log_entry)
    print(log_entry.strip())


class MQTTHealthChecker(threading.Thread):
    def __init__(self, broker):
        """
        Inicializa el thread de monitoreo para un broker específico.

        Parámetros:
            broker (dict): Diccionario con 'address' y 'port' del broker.
        """
        super().__init__()
        self.broker = broker
        self.client = mqtt.Client(client_id=f"HealthCheck-{broker['address']}")
        self.connected = False
        self.stop_event = threading.Event()

        # Asignar callbacks
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect

    def on_connect(self, client, userdata, flags, rc):
        """
        Callback cuando el cliente se conecta al broker MQTT.
        """
        if rc == 0:
            self.connected = True
            log_health_status(self.broker, "CONNECTED", "Conexión establecida exitosamente.")
        else:
            self.connected = False
            log_health_status(self.broker, "DISCONNECTED", f"Código de retorno {rc}.")

    def on_disconnect(self, client, userdata, rc):
        """
        Callback cuando el cliente se desconecta del broker MQTT.
        """
        self.connected = False
        if rc != 0:
            log_health_status(self.broker, "DISCONNECTED", f"Desconexión inesperada con codigo {rc}.")
        else:
            log_health_status(self.broker, "DISCONNECTED", "Desconexión voluntaria.")

    def run(self):
        """
        Ejecuta el ciclo de monitoreo.
        """
        while not self.stop_event.is_set():
            try:
                if not self.connected:
                    self.client.connect(self.broker['address'], self.broker['port'], keepalive=60)
                    self.client.loop_start()
                    # Esperar brevemente para establecer la conexión
                    time.sleep(2)

                if self.connected:
                    # Publicar mensaje de PING
                    result = self.client.publish("health/check", "PING")
                    status = result[0]
                    if status == mqtt.MQTT_ERR_SUCCESS:
                        log_health_status(self.broker, "CONNECTED",
                                          f"Mensaje 'PING' enviado correctamente. ({time.ctime()})")
                    else:
                        log_health_status(self.broker, "DISCONNECTED",
                                          f"Fallo al enviar mensaje 'PING'. Codigo: {status}")

                # Esperar el intervalo antes del siguiente chequeo
                time.sleep(CHECK_INTERVAL)
            except Exception as e:
                log_health_status(self.broker, "DISCONNECTED", f"Excepcion: {e}")
                self.connected = False
                self.client.loop_stop()
                time.sleep(5)  # Esperar antes de intentar reconectar

    def stop(self):
        """
        Detiene el thread de monitoreo.
        """
        self.stop_event.set()
        self.client.loop_stop()
        self.client.disconnect()


def initialize_report():
    """
    Inicializa el archivo de reporte con una cabecera.
    """
    with report_lock:
        with open(REPORT_FILE, 'w') as f:
            f.write("=== Health Check Report ===\n")
            f.write(f"Fecha de inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")


def main():
    """
    Función principal para iniciar el monitoreo de brokers MQTT.
    """
    initialize_report()
    log_health_status({'address': 'START', 'port': 0}, "INICIO", "Iniciando Health Check para brokers MQTT.")

    # Crear y empezar un thread de monitoreo para cada broker
    checkers = []
    for broker in MQTT_BROKERS:
        checker = MQTTHealthChecker(broker)
        checker.start()
        checkers.append(checker)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nDeteniendo Health Check...")
        for checker in checkers:
            checker.stop()
        for checker in checkers:
            checker.join()
        log_health_status({'address': 'END', 'port': 0}, "FIN", "Health Check detenido.")


if __name__ == "__main__":
    main()
