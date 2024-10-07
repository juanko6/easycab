import socket
import sys
import threading
from kafka import KafkaProducer
import time

HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"

# Función para enviar estados del taxi a Kafka
def enviar_estado_kafka(taxi_id, producer):
    estados = ["disponible", "en camino", "ocupado", "fuera de servicio"]
    while True:
        for estado in estados:
            mensaje = f"Taxi {taxi_id}: {estado}"
            producer.send('estados-taxis', value=mensaje.encode('utf-8'))
            print(f"Enviando estado del taxi: {mensaje}")
            time.sleep(5)  # Simular cambio de estado cada 5 segundos


def send(client, msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)

class EC_DE:
    def __init__(self, ID, PORT_SENSOR, IP_SENSOR):
        self.ID = ID
        self.estado = 0     #0=Posicion final, 1=En movimiento, 2=Parado
        self.Posicion = [1,1]
        self.PORT_SENSOR = PORT_SENSOR
        self.IP_SENSOR = IP_SENSOR

    def conectar_central(self, ADDR_CENTRAL):
        result = 0
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(ADDR_CENTRAL)
        print(f"Establecida conexión en [{ADDR_CENTRAL}]")

        print(f"Envio al servidor: Nuevo Taxi {self.ID}")
        send(client, str(self.ID))
        msgResult = client.recv(2048).decode(FORMAT)
        print("Recibo del Servidor: ", msgResult)

        result = int(msgResult)

        if result == 1:
            print("Taxi autenticado correctamente.")
        elif result == -1:
            print("Taxi con mismo ID ya creado.")
        elif result == -2:
            print("ID fuera de rango. Solo permitido [0..99].")
        else:
            print("Error en la autenticación del taxi.")

        client.close()
        return result

    def escuchar_sensor(self):
        # EC_DE escucha los mensajes del sensor en el puerto especificado
        while True:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                    # Permitir reutilización de la dirección y el puerto
                    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    server_socket.bind((self.IP_SENSOR, self.PORT_SENSOR))
                    server_socket.listen()
                    print(f"[LISTENING] EC_DE esperando conexión del sensor en {self.IP_SENSOR}:{self.PORT_SENSOR}")

                    conn, addr = server_socket.accept()  # Esperar a que el sensor se conecte
                    print(f"[CONNECTED] Sensor conectado desde {addr}")
                    with conn:
                        while True:
                            message = conn.recv(HEADER).decode(FORMAT)
                            if not message:
                                break
                            print(f"Mensaje recibido de EC_S: {message}")

            except (ConnectionResetError, OSError) as e:
                # El EC_DE sigue esperando reconexión sin detener la ejecución
                print(f"Conexión con el sensor perdida o error. Esperando reconexión... {e}")
                time.sleep(5)  # Espera unos segundos antes de intentar nuevamente

########## MAIN ##########

print("***** [EC_DE] *****")

if len(sys.argv) == 6:
    SERVER_CENTRAL = sys.argv[1]
    PORT_CENTRAL = int(sys.argv[2])
    ADDR_CENTRAL = (SERVER_CENTRAL, PORT_CENTRAL)
    ID = int(sys.argv[3])
    PORT_SENSOR = int(sys.argv[4])  # Puerto para escuchar al sensor
    IP_SENSOR = sys.argv[5]  # IP del sensor

    taxi = EC_DE(ID, PORT_SENSOR, IP_SENSOR)
    
    # Si la autenticación con la central es exitosa, escuchar al sensor
    if taxi.conectar_central(ADDR_CENTRAL) > 0:
        # Iniciar la conexión con Kafka para enviar el estado del taxi
        producer = KafkaProducer(bootstrap_servers='192.168.1.147:9092')  # Configurar Kafka Producer
        threading.Thread(target=enviar_estado_kafka, args=(taxi.ID, producer)).start()


        # Escuchar al sensor
        threading.Thread(target=taxi.escuchar_sensor).start()

else:
    print("Oops!. Parece que algo falló. Necesito estos argumentos: <ServerIP> <Puerto> <ID> <PuertoSensor> <IPSensor>")
