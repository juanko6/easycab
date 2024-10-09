import socket
import sys
import threading
from kafka import KafkaProducer
import time
import signal
import random

BOOTSTRAP_SERVER = '192.168.1.147:9092'
HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"
MAPA_DIM = 20  # Dimensión del mapa (20x20)

# Función para enviar la posición y el estado del taxi a Kafka
def enviar_posicion_estado_kafka(taxi_id, posicion, estado, producer, topic):
    mensaje = f"Taxi {taxi_id}: Posicion {posicion}, Estado {estado}"
    producer.send(topic, value=mensaje.encode('utf-8'))
    print(f"Enviando posición y estado del taxi: {mensaje}")
    producer.flush()  # Asegura que el mensaje se envía inmediatamente


# Función para leer el estado del sensor desde el archivo
def leer_estado_sensor():
    try:
        with open("estado_sensor.txt", "r") as file:
            return file.read().strip()
    except FileNotFoundError:
        return "OK"


# Función para enviar mensajes al servidor central
def send(client, msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)

class EC_DE:
    def __init__(self, ID, producer):
        self.ID = ID
        self.estado = "Disponible"  # Estado inicial del taxi
        self.posicion = [0, 0]  # Inicializar la posición del taxi en (1,1)
        self.producer = producer
        self.topic = f"TAXI_{self.ID}"
        self.running = True  # Variable para controlar el ciclo de ejecución

    def conectar_central(self, ADDR_CENTRAL):
        result = 0
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(ADDR_CENTRAL)
            print(f"Conexión establecida con la central en {ADDR_CENTRAL}")

            # Enviar el ID del taxi para autenticarse
            print(f"Enviando al servidor: Nuevo Taxi {self.ID}")
            send(client, f"Nuevo Taxi {self.ID}")
            msgResult = client.recv(2048).decode(FORMAT)
            print(f"Respuesta de la central: {msgResult}")

            result = int(msgResult)

            if result == 1:
                print("Taxi autenticado correctamente.")
            elif result == -1:
                print("Taxi con ID duplicado.")
            elif result == -2:
                print("ID fuera de rango. Solo permitido [0..99].")
            else:
                print("Error en la autenticación del taxi.")

            client.close()
        except Exception as e:
            print(f"Error al conectar con la central: {e}")

        return result

    def mover_taxi(self):
        direcciones = ["norte", "sur", "este", "oeste"]

        while self.running:
            direccion = random.choice(direcciones)
            if direccion == "norte":
                self.posicion[0] = (self.posicion[0] - 1) % MAPA_DIM  # Conectar el norte con el sur
            elif direccion == "sur":
                self.posicion[0] = (self.posicion[0] + 1) % MAPA_DIM
            elif direccion == "este":
                self.posicion[1] = (self.posicion[1] + 1) % MAPA_DIM  # Conectar el este con el oeste
            elif direccion == "oeste":
                self.posicion[1] = (self.posicion[1] - 1) % MAPA_DIM

            # Enviar la nueva posición y el estado a Kafka
            enviar_posicion_estado_kafka(self.ID, self.posicion, self.estado, self.producer, self.topic)

            time.sleep(5)  # Simular movimiento cada 5 segundos

    # Función que actualiza el estado del taxi basado en el sensor
    def actualizar_estado(self):
        while self.running:
            estado_sensor = leer_estado_sensor()

            # Cambiar el estado del taxi según el estado del sensor
            if estado_sensor == "OK":
                self.estado = "Disponible"
            else:
                self.estado = "KO"

            # Enviar la nueva posición y el estado a Kafka
            enviar_posicion_estado_kafka(self.ID, self.posicion, self.estado, self.producer, self.topic)

            time.sleep(1)  # Leer el estado del sensor cada segundo

    # Función para detener el taxi de forma ordenada
    def detener(self):
        print("\n[EC_DE] Apagando el taxi...")
        self.running = False

########## MAIN ##########

def signal_handler(sig, frame):
    print("\n[EC_DE] Señal de interrupción recibida. Finalizando...")
    taxi.detener()
    sys.exit(0)

if len(sys.argv) == 4:
    SERVER_CENTRAL = sys.argv[1]
    PORT_CENTRAL = int(sys.argv[2])
    ADDR_CENTRAL = (SERVER_CENTRAL, PORT_CENTRAL)
    ID = int(sys.argv[3])

    print(f"***** [EC_DE] ***** Iniciando Taxi ID: {ID} con Kafka en {BOOTSTRAP_SERVER}")

    # Crear el Kafka Producer
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)

    # Crear el taxi
    taxi = EC_DE(ID, producer)

    # Si la autenticación con la central es exitosa, iniciar el proceso de estados y sensor
    if taxi.conectar_central(ADDR_CENTRAL) > 0:
        # Iniciar el manejo de señales para cerrar el taxi con Ctrl+C
        signal.signal(signal.SIGINT, signal_handler)

        # Iniciar la actualización del estado del taxi
        hilo_estado = threading.Thread(target=taxi.actualizar_estado)
        hilo_estado.start()

        # Iniciar el movimiento del taxi
        hilo_movimiento = threading.Thread(target=taxi.mover_taxi)
        hilo_movimiento.start()

        # Mantener el proceso activo
        while True:
            time.sleep(1)

else:
    print("Oops!. Parece que algo falló. Necesito estos argumentos: <ServerIP> <Puerto> <ID> <PuertoSensor> <IPSensor>")