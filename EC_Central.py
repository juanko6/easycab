import socket
import sys
import threading
from kafka import KafkaConsumer

HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 5

# Lista para almacenar los IDs de taxis registrados
taxisList = []

def handle_client(conn, addr):
    print(f"[NUEVA CONEXION] {addr} connected.")

    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            print(f"He recibido del taxi [{addr}] el mensaje: {msg}")

            # Verificar el ID del taxi
            result = autenticar_taxi(int(msg))

            if result == 1:
                print(f"Taxi con ID {msg} autenticado correctamente.")
                conn.send("1".encode(FORMAT))
            elif result == -1:
                print(f"Taxi con ID {msg} ya registrado.")
                conn.send("-1".encode(FORMAT))
            elif result == -2:
                print(f"ID {msg} fuera de rango.")
                conn.send("-2".encode(FORMAT))
            else:
                conn.send("-3".encode(FORMAT))

            if msg == FIN:
                connected = False

    conn.close()

# Función para autenticar el ID del taxi
def autenticar_taxi(idTaxi):
    if 0 <= idTaxi <= 99:  # Verificar si el ID está en el rango válido
        if idTaxi not in taxisList:  # Verificar si el ID ya ha sido registrado
            taxisList.append(idTaxi)
            return 1  # ID autenticado correctamente
        else:
            return -1  # ID ya registrado
    else:
        return -2  # ID fuera de rango

def start():
    server.listen()
    print(f"[LISTENING] Servidor a la escucha en {ADDR}")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()


# Función para consumir los mensajes de Kafka (estados de los taxis)
def consumir_estados_taxis():
    # Configuración del Kafka Consumer
    consumer = KafkaConsumer(
        'estados-taxis',
        bootstrap_servers='192.168.1.147:9092',
        auto_offset_reset='earliest',
        group_id='central-group'
    )
    
    print("Esperando estados de los taxis...")
    for mensaje in consumer:
        print(f"Estado recibido de Kafka: {mensaje.value.decode('utf-8')}")

def consumir_solicitudes_clientes():
    # Configuración del Kafka Consumer
    consumer = KafkaConsumer(
        'solicitudes-clientes',
        bootstrap_servers='192.168.1.147:9092',
        auto_offset_reset='earliest',
        group_id='central-group'
    )
    
    print("Esperando solicitudes de clientes...")
    for mensaje in consumer:
        print(f"Solicitud recibida: {mensaje.value.decode('utf-8')}")
        # Aquí puedes añadir la lógica para asignar un taxi disponible


# MAIN
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python EC_Central.py <PUERTO_CENTRAL>")
        sys.exit(1)

    PORT_CENTRAL = int(sys.argv[1])
    
    SERVER = socket.gethostbyname(socket.gethostname())
    ADDR = (SERVER, PORT_CENTRAL)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)

    print("***** [EC_Central] *****")

    
    # Iniciar el hilo que consumirá los mensajes de Kafka
    threading.Thread(target=consumir_estados_taxis).start()
    threading.Thread(target=consumir_solicitudes_clientes).start()

    # Iniciar el servidor de la central para conexiones de taxis
    start()
