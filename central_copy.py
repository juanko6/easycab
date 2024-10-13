import socket
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import threading
import time
import sys
import os
from dashboard import Dashboard


BOOTSTRAP_SERVER = '192.168.1.147:9092'
GROUP_ID = 'central-group'
REFRESH_INTERVAL = 10  # Intervalo en segundos para verificar nuevos topics
HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"
FICHERO_TAXIS = "taxis_disponibles.txt"
FICHERO_MAPA = "mapa_ciudad.txt"
FICHERO_CLIENTES = "clientes_solicitudes.txt"

# Lista de taxis disponibles y taxis autenticados
taxis_disponibles = {}
taxi_ids = {}  # Taxi_ID : Conexión


# Función para cargar taxis desde el fichero
def cargar_taxis_disponibles():
    if os.path.exists(FICHERO_TAXIS):
        with open(FICHERO_TAXIS, "r") as file:
            for line in file:
                taxi_id, estado = line.strip().split(",")
                taxis_disponibles[taxi_id] = estado
    else:
        with open(FICHERO_TAXIS, "w") as file:
            file.write("")

# Función para guardar taxis en el fichero
def guardar_taxis_disponibles():
    with open(FICHERO_TAXIS, "w") as file:
        for taxi_id, estado in taxis_disponibles.items():
            file.write(f"{taxi_id},{estado}\n")

# Función para gestionar solicitudes de clientes desde el fichero
def procesar_solicitudes_clientes():
    if os.path.exists(FICHERO_CLIENTES):
        with open(FICHERO_CLIENTES, "r") as file:
            for solicitud in file:
                cliente_id, _ = solicitud.strip().split(": ")
                print(f"Solicitud recibida del cliente {cliente_id}")
                
                if taxis_disponibles:
                    taxi_id_disponible = list(taxis_disponibles.keys())[0]
                    asignar_taxi(taxi_id_disponible, solicitud.strip(), cliente_id)
                    # Marcar el taxi como ocupado
                    del taxis_disponibles[taxi_id_disponible]
                    guardar_taxis_disponibles()
                else:
                    enviar_respuesta_cliente(cliente_id, "KO")  # Enviar KO si no hay taxis
                    print("No hay taxis disponibles en este momento")
                    
        # Vaciar el fichero de solicitudes después de procesarlas
        open(FICHERO_CLIENTES, "w").close()
    else:
        open(FICHERO_CLIENTES, "w").close()

# Función para asignar un taxi a una solicitud
def asignar_taxi(taxi_id, solicitud, cliente_id):
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
    topic_taxi = f"TAXI_{taxi_id}"
    producer.send(topic_taxi, value=solicitud.encode('utf-8'))
    print(f"Solicitud {solicitud} enviada al taxi {taxi_id}")
    producer.flush()
    
    # Enviar OK al cliente
    enviar_respuesta_cliente(cliente_id, "OK")

# Función para enviar la respuesta al cliente
def enviar_respuesta_cliente(cliente_id, respuesta):
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
    topic_cliente = f'cliente_{cliente_id}'
    producer.send(topic_cliente, value=respuesta.encode('utf-8'))
    print(f"Enviando respuesta {respuesta} al cliente {cliente_id}")
    producer.flush()


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
# Función para consumir mensajes de todos los taxis
def consumir_taxis():
    consumer = KafkaConsumer(
        'estados-taxis',
        bootstrap_servers=BOOTSTRAP_SERVER,
        group_id=GROUP_ID,
        auto_offset_reset='earliest'
    )
    
    global nuevos_estados
    for mensaje in consumer:
        taxi_id, estado = mensaje.value.decode('utf-8').split(": ")
        print(f"Estado recibido de Taxi {taxi_id}: {estado}")
        nuevos_estados[taxi_id] = estado  # Actualizamos la variable con los nuevos estados


# Función para actualizar el dashboard con nuevos estados
def actualizar_dashboard(dashboard):
    global nuevos_estados
    if nuevos_estados:
        for taxi_id, estado in nuevos_estados.items():
            dashboard.actualizar_estado_taxi(int(taxi_id), estado)  # Actualizamos el estado en el dashboard
        nuevos_estados = {}  # Limpiamos los estados procesados
    dashboard.after(1000, actualizar_dashboard, dashboard)  # Repetir cada segundo

########## MAIN ##########

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python EC_Central.py <IP_Central> <Puerto_Central>")
        sys.exit(1)

    IP_CENTRAL = sys.argv[1]
    PORT_CENTRAL = int(sys.argv[2])

    print(f"***** [EC_Central] ***** Iniciando con IP: {IP_CENTRAL} y Puerto: {PORT_CENTRAL}")

    # Cargar los taxis disponibles al iniciar la central
    cargar_taxis_disponibles()

    # Iniciar el servidor de autenticación por socket en un hilo separado
    hilo_servidor = threading.Thread(target=iniciar_servidor, args=(IP_CENTRAL, PORT_CENTRAL))
    hilo_servidor.daemon = True
    hilo_servidor.start()

    # Iniciar el consumidor de Kafka en un hilo separado
    hilo_consumidor = threading.Thread(target=consumir_taxis)
    hilo_consumidor.daemon = True
    hilo_consumidor.start()

    # Iniciar el dashboard en el hilo principal
    dashboard = Dashboard()
    dashboard.after(1000, actualizar_dashboard, dashboard)
    dashboard.mainloop()

    