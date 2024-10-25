import socket
import sys
import threading
from kafka import KafkaProducer
import time
import signal
import random
import configuracion

BOOTSTRAP_SERVER = configuracion.Entorno()
HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"
MAPA_DIM = 20  # Dimensión del mapa (20x20)

def calcular_lrc(estado):
    lrc = 0
    for byte in estado.encode():
        lrc ^= byte
    return lrc

# Función para desempaquetar el estado
def desempaquetar_estado(estado):
    stx_index = estado.find("<STX>")
    etx_index = estado.find("<ETX>")
    if stx_index != -1 and etx_index != -1:
        data = estado[stx_index + len("<STX>"):etx_index]
        lrc = int(estado[etx_index + len("<ETX>"):])
        return data, lrc
    else:
        return None, None

def verificar_lrc(estado, lrc_recibido):
    lrc_calculado = calcular_lrc(estado)
    return lrc_calculado == lrc_recibido

# Función para manejar la recepción de mensajes del sensor
def manejar_sensor(conn_sensor):
    while True:
        try:
            # Recibir estado del sensor
            estado_sensor = conn_sensor.recv(4096).decode(FORMAT)
            if not estado_sensor:
                print("Cliente desconectado.")
                break

            if estado_sensor == "<EOT>":
                print("Cliente envió <EOT>. Finalizando comunicación.")
                break

            print(f"Estado recibido del sensor: {estado_sensor}")

            # Desempaquetar el mensaje
            data, lrc_recibido = desempaquetar_estado(estado_sensor)
            if data is None:
                print("Error en el formato del mensaje")
                conn_sensor.send("<NACK>".encode())
                continue

            # Verificar LRC
            if verificar_lrc(estado_sensor[:estado_sensor.find("<ETX>")+len("<ETX>")], lrc_recibido):
                print(f"estado válido: {data}")
                conn_sensor.send("<ACK>".encode())
                # Guardar el estado en un fichero (sobrescribir)
                with open("estado_sensor.txt", "w") as file:
                    file.write(f"{data}\n")
            else:
                print("Error: LRC no coincide. El mensaje está corrupto.")
                conn_sensor.send("<NACK>".encode())
        
        except ConnectionResetError:
            print("El cliente cerró la conexión inesperadamente.")
            break

        except Exception as e:
            print(f"Error al recibir estado del sensor: {e}")
            break
    conn_sensor.close()
    print("Conexión cerrada con el cliente.")
    print(f"Esperando conexion del sensor en {SENSOR_IP}:{SENSOR_PORT}...")


# Función para enviar la posición y el estado del taxi a Kafka
def enviar_posicion_estado_kafka(taxi_id, posicion, estado, producer, topic):
    mensaje = f"Taxi {taxi_id}: Posicion {posicion}, Estado {estado}"
    producer.send(topic, value=mensaje.encode('utf-8'))
    #print(f"Enviando posición y estado del taxi: {mensaje}")
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

# Función para aceptar conexiones y manejar las desconexiones de EC_S
# Función para aceptar conexiones y manejar las desconexiones de EC_S
def aceptar_conexiones(server_socket):
    while taxi.running:  # Verificar que taxi siga corriendo
        try:
            server_socket.settimeout(2.0)  # Establecer un timeout de 2 segundos en el accept()
            print("Esperando conexion del sensor...")
            conn_sensor, addr = server_socket.accept()
            print(f"Conexión establecida con el sensor en {addr}")

            # Confirmar conexión con <ACK> después de recibir <ENQ>
            enq = conn_sensor.recv(1024).decode()
            if enq == "<ENQ>":
                conn_sensor.send("<ACK>".encode())
                print("Conexión confirmada con <ACK>")

                # Iniciar el manejo de mensajes del sensor en un hilo separado
                hilo_sensor = threading.Thread(target=manejar_sensor, args=(conn_sensor,))
                hilo_sensor.start()

            else:
                conn_sensor.send("<NACK>".encode())
                print("Error: Conexión no válida. Se ha enviado <NACK>.")
                conn_sensor.close()

        except OSError:
            print("Socket cerrado. Saliendo del bucle de conexiones.")
            break

        except Exception as e:
            print(f"Error al establecer la conexión con el sensor: {e}")


########## MAIN ##########

def signal_handler(sig, frame):
    print("\n[EC_DE] Señal de interrupción recibida. Finalizando...")
    taxi.detener()  # Detener el taxi y sus hilos
    if server_socket:
        server_socket.close()  # Cerrar el socket del servidor para desbloquear el accept()
    sys.exit(0)  # Terminar el programa

if len(sys.argv) == 6:
    # Leer los argumentos
    SERVER_CENTRAL = sys.argv[1]
    PORT_CENTRAL = int(sys.argv[2])
    ADDR_CENTRAL = (SERVER_CENTRAL, PORT_CENTRAL)
    ID = int(sys.argv[3])

    SENSOR_IP = sys.argv[4]  # IP del sensor (EC_S)
    SENSOR_PORT = int(sys.argv[5])  # Puerto del sensor (EC_S)

    print(f"***** [EC_DE] ***** Iniciando Taxi ID: {ID} con Kafka en {BOOTSTRAP_SERVER}")
    print(f"Esperando conexion del sensor en {SENSOR_IP}:{SENSOR_PORT}...")

    # Crear el Kafka Producer
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)

    # Crear el taxi
    taxi = EC_DE(ID, producer)

    # Si la autenticación con la central es exitosa, iniciar el proceso de estados y sensor
    if taxi.conectar_central(ADDR_CENTRAL) > 0:
        # Iniciar el manejo de señales para cerrar el taxi con Ctrl+C
        signal.signal(signal.SIGINT, signal_handler)

        # Iniciar la actualización del estado del taxi en un hilo separado
        hilo_estado = threading.Thread(target=taxi.actualizar_estado)
        hilo_estado.start()

        # Iniciar el movimiento del taxi en un hilo separado
        hilo_movimiento = threading.Thread(target=taxi.mover_taxi)
        hilo_movimiento.start()

        # Iniciar el servidor para recibir estados de los sensores en un hilo separado
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((SENSOR_IP, SENSOR_PORT))
        server_socket.listen(1)

        # Crear el hilo para aceptar conexiones
        hilo_servidor = threading.Thread(target=aceptar_conexiones, args=(server_socket,))
        hilo_servidor.start()

        # Mantener el proceso activo sin bloqueos
        while True:
            time.sleep(1)

else:
    print("Oops!. Parece que algo falló. Necesito estos argumentos: <ServerIP> <Puerto> <ID> <IPSensor> <PuertoSensor>")