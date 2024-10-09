import socket
from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
import threading
import time
import sys

BOOTSTRAP_SERVER = '192.168.1.147:9092'
GROUP_ID = 'central-group'
REFRESH_INTERVAL = 10  # Intervalo en segundos para verificar nuevos topics
HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"

# Lista de taxis disponibles y taxis autenticados
taxis_disponibles = {}
taxi_ids = {}  # Taxi_ID : Conexión

def nuevo_taxi(conn, addr):
    print(f"[NUEVA CONEXIÓN] {addr} connected.")
    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            print(f"He recibido del taxi [{addr}] el mensaje: {msg}")

            # Extraer el ID del taxi del mensaje
            if msg.startswith("Nuevo Taxi"):
                try:
                    taxi_id = int(msg.split()[2])  # Extraer el ID como entero
                except (IndexError, ValueError):
                    conn.send("-3".encode(FORMAT))  # Mensaje malformado
                    connected = False
                    continue

                # Verificar el ID del taxi
                result = autenticar_taxi(taxi_id)

                if result == 1:
                    print(f"Taxi con ID {taxi_id} autenticado correctamente.")
                    conn.send("1".encode(FORMAT))
                elif result == -1:
                    print(f"Taxi con ID {taxi_id} ya registrado.")
                    conn.send("-1".encode(FORMAT))
                elif result == -2:
                    print(f"ID {taxi_id} fuera de rango.")
                    conn.send("-2".encode(FORMAT))
                else:
                    conn.send("-3".encode(FORMAT))
            else:
                print("Mensaje inesperado recibido.")
                conn.send("-3".encode(FORMAT))

            if msg == FIN:
                connected = False

    conn.close()

# Función para autenticar el ID del taxi y crear el topic si no existe
def autenticar_taxi(idTaxi):
    if 0 <= idTaxi <= 99:  # Verificar si el ID está en el rango válido
        if idTaxi not in taxis_disponibles:  # Verificar si el ID ya ha sido registrado
            taxis_disponibles[idTaxi] = "Disponible"
            
            # Crear el topic para el taxi si no existe
            admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVER)
            topic_name = f"TAXI_{idTaxi}"
            topics = admin_client.list_topics()

            if topic_name not in topics:
                topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
                admin_client.create_topics([topic])
                print(f"Topic {topic_name} creado exitosamente.")

            return 1  # ID autenticado correctamente
        else:
            return -1  # ID ya registrado
    else:
        return -2  # ID fuera de rango

# Función para iniciar el servidor de autenticación
def iniciar_servidor(IP_CENTRAL, PORT_CENTRAL):
    ADDR = (IP_CENTRAL, PORT_CENTRAL)
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)
    server.listen()
    print(f"[LISTENING] Servidor a la escucha en {IP_CENTRAL}:{PORT_CENTRAL}")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=nuevo_taxi, args=(conn, addr))
        thread.start()
        print(f"[CONEXIONES ACTIVAS] {threading.active_count() - 1}")

# Función para listar y suscribirse a todos los topics que empiecen con TAXI_
def obtener_topics_taxi():
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVER, client_id='admin-central')
    topics = admin_client.list_topics()

    # Filtrar los topics que empiecen con "TAXI_"
    topics_taxi = [topic for topic in topics if topic.startswith('TAXI_')]
    return topics_taxi

# Función para consumir mensajes de todos los taxis
def consumir_taxis():
    current_topics = set()
    consumer = None  # Inicializar el consumidor como None

    while True:
        new_topics = set(obtener_topics_taxi())

        if new_topics != current_topics:  # Si hay nuevos topics, actualizamos el consumidor
            current_topics = new_topics

            if consumer is not None:
                consumer.close()  # Cerrar el consumidor anterior si existe

            if current_topics:
                consumer = KafkaConsumer(
                    *current_topics,  # Pasar la lista de topics como argumentos
                    bootstrap_servers=BOOTSTRAP_SERVER,
                    group_id=GROUP_ID,
                    auto_offset_reset='earliest'
                )
                print(f"Escuchando los topics: {current_topics}")

                # Consumir mensajes en un bucle
                for mensaje in consumer:
                    taxi_id, estado = mensaje.value.decode('utf-8').split(": ")
                    print(f"Estado recibido: {mensaje.value.decode('utf-8')}")

                    # Si el taxi está disponible, lo añadimos a la lista
                    if estado == "Disponible":
                        taxis_disponibles[taxi_id] = estado
                        print(f"Taxi {taxi_id} disponible")
                    elif estado == "KO":
                        # Si el taxi tiene una incidencia, lo eliminamos de la lista de disponibles
                        if taxi_id in taxis_disponibles:
                            del taxis_disponibles[taxi_id]
                        print(f"Taxi {taxi_id} fuera de servicio")
            else:
                print("No se encontraron topics de taxis disponibles en Kafka.")

        time.sleep(REFRESH_INTERVAL)  # Revisar nuevos topics cada 10 segundos

# Función para recibir solicitudes de clientes y asignar un taxi
def recibir_solicitudes():
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
    
    while True:
        solicitud_cliente = input("Ingrese la solicitud del cliente (ej: Solicito taxi en Calle 10): ")

        if taxis_disponibles:
            # Seleccionamos el primer taxi disponible de la lista
            taxi_id_disponible = list(taxis_disponibles.keys())[0]
            topic_taxi = f"TAXI_{taxi_id_disponible}"

            # Enviar la solicitud al topic del taxi
            producer.send(topic_taxi, value=solicitud_cliente.encode('utf-8'))
            print(f"Solicitud enviada al taxi {taxi_id_disponible}: {solicitud_cliente}")
            
            # Marcamos el taxi como ocupado, eliminándolo de la lista de disponibles
            del taxis_disponibles[taxi_id_disponible]
        else:
            print("No hay taxis disponibles en este momento")

########## MAIN ##########

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python EC_Central.py <IP_Central> <Puerto_Central>")
        sys.exit(1)

    IP_CENTRAL = sys.argv[1]
    PORT_CENTRAL = int(sys.argv[2])

    print(f"***** [EC_Central] ***** Iniciando con IP: {IP_CENTRAL} y Puerto: {PORT_CENTRAL}")

    # Iniciar el servidor de autenticación por socket en un hilo separado
    hilo_servidor = threading.Thread(target=iniciar_servidor, args=(IP_CENTRAL, PORT_CENTRAL))
    hilo_servidor.daemon = True
    hilo_servidor.start()

    # Iniciar el consumidor de Kafka en un hilo separado
    hilo_consumidor = threading.Thread(target=consumir_taxis)
    hilo_consumidor.daemon = True
    hilo_consumidor.start()

    # Escuchar solicitudes de clientes y asignar taxis
    recibir_solicitudes()
