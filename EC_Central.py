import socket
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import threading
import time
import sys
import os
from dashboard import Dashboard
import random
import configuracion


BOOTSTRAP_SERVER = configuracion.Entorno()
GROUP_ID = 'central-group'
REFRESH_INTERVAL = 2  # Intervalo en segundos para verificar nuevos topics
HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"
FICHERO_TAXIS = "taxis_disponibles.txt"
FICHERO_MAPA = "mapa_ciudad.txt"
DB_TAXIS = "taxis_db.txt"  # Fichero que actuará como base de datos

MAPA_FILAS = 20
MAPA_COLUMNAS = 20

# Lista de taxis disponibles y taxis autenticados
taxis_disponibles = {}
taxi_ids = {}  # Taxi_ID : Conexión
nuevos_estados = {} 
clientes = {}

# Función para escribir las posiciones y estados de los taxis en el fichero
def guardar_en_fichero(taxi_id, posicion=None, estado=None):
    lineas = []
    taxi_encontrado = False

    try:
        with open(DB_TAXIS, "r") as file:
            lineas = file.readlines()
    except FileNotFoundError:
        with open(DB_TAXIS, "w") as file:
            file.write("TaxiID,Posicion,Estado\n")
        lineas = ["TaxiID,Posicion,Estado\n"]

    for idx, linea in enumerate(lineas):
        if linea.startswith(f"{taxi_id},"):
            taxi_encontrado = True
            lineas[idx] = f"{taxi_id},[{int(posicion[0])},{int(posicion[1])}],{estado}\n"
            break

    if not taxi_encontrado:
        posicion_str = f"[{int(posicion[0])},{int(posicion[1])}]" if posicion else "[0,0]"
        estado_str = estado if estado else "desconocido"
        lineas.append(f"{taxi_id},{posicion_str},{estado_str}\n")

    with open(DB_TAXIS, "w") as file:
        file.writelines(lineas)

    print(f"Datos guardados en {DB_TAXIS}: Taxi {taxi_id} - Posición {posicion} - Estado {estado}")


# Función para cargar los taxis disponibles desde el fichero
def cargar_taxis_disponibles():
    try:
        with open(DB_TAXIS, "r") as file:
            lineas = file.readlines()

        for line in lineas[1:]:  # Omitir la cabecera
            try:
                taxi_id, posicion, estado = line.strip().split(",")  # Leer TaxiID, Posicion, Estado
                posicion = list(map(int, posicion.strip("[]").split(",")))  # Convertir la posición a lista [x, y]
                taxis_disponibles[int(taxi_id)] = {"posicion": posicion, "estado": estado}
                print(f"Taxi {taxi_id} cargado: Posicion {posicion}, Estado {estado}")
            except ValueError:
                print(f"Error al procesar la línea: {line.strip()}")  # Manejar líneas mal formateadas

    except FileNotFoundError:
        print(f"El fichero {DB_TAXIS} no se encontró, inicializando vacio.")

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
def verificar_topic_creado(topic_name):
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVER)
    topics = admin_client.list_topics()
    return topic_name in topics


# Esperar activamente a que el topic esté disponible en Kafka
def esperar_topic_disponible(topic_name, timeout=10):
    start_time = time.time()
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
    while time.time() - start_time < timeout:
        if producer.partitions_for(topic_name):
            print(f"Topic {topic_name} está disponible para producción.")
            return True
        time.sleep(1)  # Esperar 1 segundo antes de volver a verificar
    print(f"Error: Topic {topic_name} no está disponible después de {timeout} segundos.")
    return False

def autenticar_taxi(idTaxi):
    if 0 <= idTaxi <= 99:  # Verificar si el ID está en el rango válido
        if idTaxi not in taxis_disponibles:  # Verificar si el ID ya ha sido registrado
            taxis_disponibles[idTaxi] = "Disponible"
            
            # Crear el topic para el taxi si no existe

            topic_name = f"TAXI_{idTaxi}"
            if not verificar_topic_creado(topic_name):
                admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVER)
                topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
                admin_client.create_topics([topic])
                print(f"Topic {topic_name} creado exitosamente.")

                # Esperar a que el topic esté disponible para la producción
                if esperar_topic_disponible(topic_name):
                    print(f"El topic {topic_name} está listo para ser usado.")
                else:
                    print(f"Error: El topic {topic_name} no está disponible después de crear.")
            else:
                print(f"Topic {topic_name} ya existe.")

            return 1  # ID autenticado correctamente
        else:
            return -1  # ID ya registrado
    else:
        return -2  # ID fuera de rango

# Función para iniciar el servidor de autenticación
def iniciar_autenticacion_taxis(IP_CENTRAL, PORT_CENTRAL):
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

# Función para consumir los mensajes desde los topics específicos de cada taxi
def consumir_posiciones_taxis():
    current_topics = set()
    consumer = None  # Inicializar el consumidor como None

    while True:
        new_topics = set(obtener_topics_taxi())  # Revisar los nuevos topics de taxis

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

                for mensaje in consumer:
                    contenido = mensaje.value.decode('utf-8')
                    if "Posicion" in contenido and "Estado" in contenido:
                        taxi_id_str, resto = contenido.split(": Posicion ")
                        taxi_id = int(taxi_id_str.split()[1])  # Obtener el ID del taxi
                        posicion_str, estado = resto.split(", Estado ")
                        posicion = list(map(int, posicion_str.strip('[]').split(',')))  # Convertir la posición a lista

                        print(f"Taxi {taxi_id} - Posición: {posicion}, Estado: {estado}")
                        guardar_en_fichero(taxi_id, posicion, estado)

        time.sleep(REFRESH_INTERVAL)



# Función para inicializar el fichero (si es necesario)
def inicializar_fichero():
    if not os.path.exists(DB_TAXIS):
        with open(DB_TAXIS, "w") as file:
            file.write("TaxiID,Posicion,Estado\n")  # Cabecera del fichero
            


# Función para actualizar el dashboard con nuevos estados
def actualizar_dashboard(dashboard):
    global nuevos_estados
    if nuevos_estados:
        for taxi_id, estado in nuevos_estados.items():
            dashboard.actualizar_estado_taxi(int(taxi_id), estado)  # Actualizamos el estado en el dashboard
        nuevos_estados = {}  # Limpiamos los estados procesados
    dashboard.after(1000, actualizar_dashboard, dashboard)  # Repetir cada segundo

#### CUSTOMER #####
def iniciar_ubicaciones_clientes():
        letras = ['a', 'b', 'c', 'd', 'e']
        ubicaciones_ocupadas = set()
        for letra in letras:
            while True:
                fila = random.randint(0, MAPA_FILAS - 1)
                columna = random.randint(0, MAPA_COLUMNAS - 1)
                ubicacion = (fila, columna)
                if ubicacion not in ubicaciones_ocupadas:
                    clientes[letra] = ubicacion
                    ubicaciones_ocupadas.add(ubicacion)
                    break

# Función para gestionar solicitudes de clientes desde el fichero
def consumir_solicitudes_clientes():        
        consumer = KafkaConsumer(f'Customer-Central',bootstrap_servers=BOOTSTRAP_SERVER, auto_offset_reset='latest')
        producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)

        for message in consumer:
            solicitud = message.value.decode('utf-8')
            print(f"Nueva solicitud: {solicitud}")    
            cliente_id, destino = solicitud.split(";")
        
            print(f"Disponibles:....{taxis_disponibles}")
            if taxis_disponibles:
                taxi_id_disponible = list(taxis_disponibles.keys())[0]
                asignar_taxi(taxi_id_disponible, destino, cliente_id)
                # Marcar el taxi como ocupado
                del taxis_disponibles[taxi_id_disponible]
            else:
                enviar_respuesta_cliente(cliente_id, "KO")  # Enviar KO si no hay taxis
                print("No hay taxis disponibles en este momento")                  

# Función para asignar un taxi a una solicitud
def asignar_taxi(taxi_id, destino, cliente_id):
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
    topic_taxi = f"TAXI_{taxi_id}"
    
        #1 - obtener ubicacion CLIENTE.
    ubicacion_Cliente = clientes[cliente_id]
    servicio = f"{ubicacion_Cliente};{destino}"
        #2 - Enviar a TAXI a por CLIENTE.
    producer.send(topic_taxi, value=servicio.encode('utf-8'))
    producer.flush()
        #3 - Esperar a que el TAXI confirme recogida del CLIENTE
    #TODO: Obtener respuesta confimación de llegada del TAXI    
    print(f"Cliente '{cliente_id}' recogido por taxi {taxi_id}") 
        #4 - Esperar a que el TAXI confirme llegada al DESTINO
    #TODO: Obtener respuesta confimación de llegada del TAXI    
    print(f"Cliente '{cliente_id}' dejado en destino {destino} por taxi {taxi_id}") 
        #5 - Poner TAXI disponible y cambiar a nueva ubicacion del CLIENTE.
    taxis_disponibles[taxi_id] = "Disponible"
    clientes[cliente_id] = destino

    # Enviar OK al cliente
    enviar_respuesta_cliente(cliente_id, "OK")

# Función para enviar la respuesta al cliente
def enviar_respuesta_cliente(cliente_id, respuesta):
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)    
    print(f"Enviando respuesta {respuesta} al cliente '{cliente_id}'")
    mensaje = f"{cliente_id};{respuesta}"
    producer.send('Central-Customer', value=mensaje.encode('utf-8'))
    producer.flush()


########## MAIN ##########

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python EC_Central.py <IP_Central> <Puerto_Central>")
        sys.exit(1)

    IP_CENTRAL = sys.argv[1]
    PORT_CENTRAL = int(sys.argv[2])
    BOOTSTRAP_SERVER = configuracion.Entorno()
    print(f"***** [EC_Central] ***** Iniciando con IP: {IP_CENTRAL} y Puerto: {PORT_CENTRAL}")
    
    # Inicializar el fichero que actuará como base de datos
    inicializar_fichero()
    # Cargar los taxis disponibles al iniciar la central
    cargar_taxis_disponibles()
    iniciar_ubicaciones_clientes()

    # Iniciar el servidor de autenticación por socket en un hilo separado
    hilo_servidor = threading.Thread(target=iniciar_autenticacion_taxis, args=(IP_CENTRAL, PORT_CENTRAL))
    hilo_servidor.daemon = True
    hilo_servidor.start()


    # Iniciar el dashboard en el hilo principal
    dashboard = Dashboard()

    # Hilo para consumir posiciones y estados
    hilo_consumir_posiciones = threading.Thread(target=consumir_posiciones_taxis)
    hilo_consumir_posiciones.daemon = True
    hilo_consumir_posiciones.start()

    # Hilo para peticiones clientes
    hilo_consumir_posiciones = threading.Thread(target=consumir_solicitudes_clientes)
    hilo_consumir_posiciones.daemon = True
    hilo_consumir_posiciones.start()

    dashboard.after(1000, actualizar_dashboard, dashboard)
    dashboard.mainloop()
