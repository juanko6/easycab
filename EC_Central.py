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
#GROUP_ID = 'central-group'
REFRESH_INTERVAL = 2  # Intervalo en segundos para verificar nuevos topics
HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"
#FICHERO_TAXIS = "taxis_disponibles.txt"
#FICHERO_MAPA = "mapa_ciudad.txt"
DB_TAXIS = "taxis_db.txt"  # Fichero que actuará como base de datos
MAX_TAXIS = configuracion.LicenciasTaxis()

MAPA_FILAS = 20
MAPA_COLUMNAS = 20

servicios_en_curso = {}  # Para trackear servicios activos
consumer_servicios = None  # Consumer global para servicios

# Lista de taxis disponibles y taxis autenticados
taxis_autenticados = set()
newTopicsTaxis = set()
taxis_disponibles = set()
taxi_ids = {}  # Taxi_ID : Conexión
nuevos_estados = {} 
clientes = {}

#####
########## PERSITENCIA DATOS ##########
#####

# Función para inicializar el fichero (si es necesario)
def inicializar_fichero():
    if not os.path.exists(DB_TAXIS):
        with open(DB_TAXIS, "w") as file:
            file.write("TaxiID;Posicion;Estado\n")  # Cabecera del fichero

# Función para escribir las posiciones y estados de los taxis en el fichero
def guardar_en_fichero(taxi_id, posicion=None, estado=None):
    lineas = []
    taxi_encontrado = False

    try:
        with open(DB_TAXIS, "r") as file:
            lineas = file.readlines()
    except FileNotFoundError:
        with open(DB_TAXIS, "w") as file:
            file.write("TaxiID;Posicion;Estado\n")
        lineas = ["TaxiID;Posicion;Estado\n"]

    for idx, linea in enumerate(lineas):
        if linea.startswith(f"{taxi_id};"):
            taxi_encontrado = True
            lineas[idx] = f"{taxi_id};[{int(posicion[0])},{int(posicion[1])}];{estado}\n"
            break

    if not taxi_encontrado:
        posicion_str = f"[{int(posicion[0])},{int(posicion[1])}]" if posicion else "[0,0]"
        estado_str = estado if estado else "desconocido"
        lineas.append(f"{taxi_id};{posicion_str};{estado_str}\n")

    with open(DB_TAXIS, "w") as file:
        file.writelines(lineas)

    #print(f"Datos guardados en {DB_TAXIS}: Taxi {taxi_id} - Posición {posicion} - Estado {estado}")


# Función para cargar los taxis disponibles desde el fichero
def cargar_taxis_disponibles():
    try:
        with open(DB_TAXIS, "r") as file:
            lineas = file.readlines()

        for line in lineas[1:]:  # Omitir la cabecera
            try:
                taxi_id, posicion, estado = line.strip().split(";")  # Leer TaxiID, Posicion, Estado
                posicion = list(map(int, posicion.strip("[]").split(",")))  # Convertir la posición a lista [x, y]
                print(f"Ultimo estado guardado: {taxi_id}, {posicion}, {estado}")
                estado = "esperandoconexion" #Al cargar desde fichero ponemos estado esperando por defecto hasta que recibamos el estado real del taxi.
                if(autenticar_taxi(int(taxi_id))>0): 
                    print(f"Taxi {taxi_id} cargado: Posicion {posicion}, Estado {estado}")
                    if(estado=="Disponible"):              
                        taxis_disponibles.add(int(taxi_id))
                    guardar_en_fichero(int(taxi_id),posicion, estado) #Guardamos el nuevo estado.
                else:
                    print(f"Antiguo taxi {taxi_id} no se ha podido autenticar.")
            except ValueError:
                print(f"Error al leer la línea: {line.strip()}")  # Manejar líneas mal formateadas

    except FileNotFoundError:
        print(f"El fichero {DB_TAXIS} no se encontró, inicializando vacio.")

#####
########## AUTENTICACION TAXIS ##########
#####

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
                elif result == 2:
                    print(f"Taxi con ID {taxi_id} ya registrado.")
                    conn.send("2".encode(FORMAT))
                elif result == -1:
                    print(f"ID {taxi_id} fuera de rango.")
                    conn.send("-1".encode(FORMAT))
                else:                    
                    print("Autencación fallida.")
                    conn.send("-2".encode(FORMAT))
            else:
                print("Mensaje inesperado recibido.")
                conn.send("-3".encode(FORMAT))

            if msg == FIN:
                connected = False

    conn.close()

def autenticar_taxi(idTaxi):
    if 0 <= idTaxi <= MAX_TAXIS:  # Verificar si el ID está en el rango válido
        if idTaxi not in taxis_autenticados:  # Verificar si el ID ya ha sido autenticado
            taxis_autenticados.add(idTaxi) 
            newTopicsTaxis.add(f"TAXI_{idTaxi}")

            return 1  # ID autenticado correctamente
        else:
            return 2  # ID ya registrado
    else:
        return -1  # ID fuera de rango

# Función para iniciar el servidor de autenticación
def iniciar_autenticacion_taxis(IP_CENTRAL, PORT_CENTRAL):    
    print("***Inicio hilo autenticación taxis***")
    ADDR = (IP_CENTRAL, PORT_CENTRAL)
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)
    server.listen()
    print(f"[AUTENTICACION] Servidor a la escucha en {IP_CENTRAL}:{PORT_CENTRAL}")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=nuevo_taxi, args=(conn, addr))
        thread.start()
        print(f"[CONEXIONES ACTIVAS] {threading.active_count() - 1}")

#####
########## COMUNICACION TAXIS ##########
#####

# Función para listar y suscribirse a todos los topics que empiecen con TAXI_
def obtener_topics_taxi():
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVER, client_id='admin-central')
    topics = admin_client.list_topics()

    # Filtrar los topics que empiecen con "TAXI_"
    topics_taxi = [topic for topic in topics if topic.startswith('TAXI_')]
    return topics_taxi

def subscribir_NuevotopicTaxi(consumer, current_topics):
    while True:
        if(newTopicsTaxis):
            current_topics.update(newTopicsTaxis)
            print(f"Nuevo Taxi detectado {newTopicsTaxis}")
            newTopicsTaxis.clear()
            consumer.subscribe(current_topics)
            print(f"Subsicrito a : {consumer.subscription()}")
        time.sleep(REFRESH_INTERVAL)
    
# Función para consumir los mensajes desde los topics específicos de cada taxi
# Función para consumir los mensajes desde los topics específicos de cada taxi
def consumir_posiciones_taxis():
    print("***Inicio hilo consumir taxis***")
    current_topics = set(obtener_topics_taxi())

    print(f"Creando consumidor para topics de TAXI: {current_topics}")
    consumer = KafkaConsumer(
                    *current_topics,
                    bootstrap_servers=BOOTSTRAP_SERVER,
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    group_id="group_taxis"                    
                )   
    thread = threading.Thread(target=subscribir_NuevotopicTaxi, args=(consumer, current_topics))
    thread.start() 

    for mensaje in consumer:
        contenido = mensaje.value.decode('utf-8')
        if "Posicion" in contenido and "Estado" in contenido:
            taxi_id_str, resto = contenido.split(": Posicion ")
            taxi_id = int(taxi_id_str.split()[1])  # Obtener el ID del taxi
            posicion_str, estado = resto.split(", Estado ")
            posicion = list(map(int, posicion_str.strip('[]').split(',')))  # Convertir la posición a lista

            # Actualizar la lista de taxis disponibles
            if taxi_id in taxis_disponibles and estado != "Disponible":   
                taxis_disponibles.discard(taxi_id)
            elif taxi_id not in taxis_disponibles and estado == "Disponible":
                taxis_disponibles.add(taxi_id)

            guardar_en_fichero(taxi_id, posicion, estado)  # Guardar estado en el fichero

# Función para actualizar el dashboard con nuevos estados
def actualizar_dashboard(dashboard):
    global nuevos_estados
    if nuevos_estados:
        for taxi_id, estado in nuevos_estados.items():
            dashboard.actualizar_estado_taxi(int(taxi_id), estado)  # Actualizamos el estado en el dashboard
        nuevos_estados = {}  # Limpiamos los estados procesados
    dashboard.after(1000, actualizar_dashboard, dashboard)  # Repetir cada segundo

#####
########## CUSTOMER ##########
#####

def iniciar_ubicacion_cliente(id, dashboard):
    # letras = ['a', 'b', 'c', 'd', 'e']
    # ubicaciones_ocupadas = set()

    if id not in clientes:
        fila = random.randint(0, MAPA_FILAS - 1)
        columna = random.randint(0, MAPA_COLUMNAS - 1)
        ubicacion = (columna, fila)
        # if ubicacion not in ubicaciones_ocupadas: //Permitimos que dos clientes puedan estar en la misma ubicación.
        if id not in clientes:
            clientes[id] = ubicacion
            dashboard.actulizarDatosCliente(id, columna, fila, "Esperando")    

    dashboard.actualizar_clientes()

def inicializar_consumer_servicios():
    #Inicializa el consumidor global para servicios de taxis
    global consumer_servicios
    consumer_servicios = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVER,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id="group_Servicios_Global"
    )

def subscribir_a_servicio_taxi(taxi_id):
    #Suscribe el consumidor global a un nuevo topic de servicio de taxi
    topic = f"ST_{taxi_id}"    
    print(f"Subsicrito a : {consumer_servicios.subscription()}")
    if(consumer_servicios.subscription()):
        current_topics = set(consumer_servicios.subscription())
    else:
        current_topics = set()
    current_topics.add(topic)
    consumer_servicios.subscribe(current_topics)
    print(f"Suscrito a topic de servicio: {topic}")

# Función para gestionar solicitudes de clientes desde el fichero
def consumir_solicitudes_clientes():         
    print("***Inicio hilo cliente***")       
    consumer = KafkaConsumer(f'Customer-Central',bootstrap_servers=BOOTSTRAP_SERVER, 
                            auto_offset_reset='latest',
                            enable_auto_commit=True,
                            group_id=f"group_Clientes")
    
    for message in consumer:
        solicitud = message.value.decode('utf-8')
        print(f"Nueva solicitud: {solicitud}")                
        cliente_id, destino = solicitud.split(";")        
        print(f"Disponibles:....{taxis_disponibles}")
        if taxis_disponibles:
            taxi_id_disponible = taxis_disponibles.pop() # Obtener taxi y marcar como no disponible
            iniciar_ubicacion_cliente(cliente_id, dashboard)
            enviar_respuesta_cliente(cliente_id, "OK")  # Enviar OK servicio aceptado.    
            asignar_taxi(taxi_id_disponible, destino, cliente_id)                         
        else:
            enviar_respuesta_cliente(cliente_id, "KO")  # Enviar KO si no hay taxis
            print("No hay taxis disponibles en este momento")                  

# Función para asignar un taxi a una solicitud
def asignar_taxi(taxi_id, destino, cliente_id):
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
    topic_taxi = f"TAXI_{taxi_id}"
        #0 - Obtener ubicación de destino
    posDetino = getPosDestino(destino)
        #1 - obtener ubicacion CLIENTE.
    ubicacion_Cliente = clientes[cliente_id]    
    columna, fila = ubicacion_Cliente
    dashboard.refrescar_cliente(cliente_id, columna, fila, "OK. Sin Taxi") 

    # Registrar el servicio en curso
    servicios_en_curso[taxi_id] = {
        'cliente_id': cliente_id,
        'origen': ubicacion_Cliente,
        'destino': destino,
        'pos_destino': posDetino
    }

    subscribir_a_servicio_taxi(taxi_id)

        #2 - Enviar TAXI a por CLIENTE.
    servicio = f"{ubicacion_Cliente};{posDetino}"
    producer.send(topic_taxi, value=servicio.encode('utf-8'))
    producer.flush()
    print(f"Servicio para {topic_taxi}: {servicio}")

    #3 - Esperar a que el TAXI informe estado del servicio
#    thread = threading.Thread(target=eseprar_servicio_taxi, args=(taxi_id, cliente_id, posDetino, destino))
#    thread.start()  

#def eseprar_servicio_taxi(taxi_id, cliente_id, posDetino, destino):
def procesar_mensajes_servicios():
    #Procesa los mensajes de servicios de todos los taxis en curso
    print("***Inicio hilo procesar servicios***")
    while True:
        mensaje = consumer_servicios.poll(timeout_ms=1000)
        for topic_partition, mensajes in mensaje.items():
            for msg in mensajes:
                topic = topic_partition.topic
                taxi_id = int(topic.split('_')[1])
                if taxi_id in servicios_en_curso:
                    contenido = int(msg.value.decode('utf-8'))
                    servicio = servicios_en_curso[taxi_id]
                    
                    if contenido == 1:  # taxi recoge al cliente
                        cliente_id = servicio['cliente_id']
                        columna, fila = servicio['origen']
                        dashboard.refrescar_cliente(cliente_id, columna, fila, f"OK. Taxi {taxi_id}")
                        print(f"Cliente '{cliente_id}' recogido por taxi {taxi_id}")
                    
                    elif contenido == 2:  # taxi confirma llegada al destino
                        cliente_id = servicio['cliente_id']
                        destino = servicio['destino']
                        pos_destino = servicio['pos_destino']
                        print(f"Cliente '{cliente_id}' dejado en destino {destino} por taxi {taxi_id}")
                        taxis_disponibles.add(taxi_id)
                        clientes[cliente_id] = pos_destino
                        columna, fila = pos_destino
                        dashboard.refrescar_cliente(cliente_id, columna, fila, "FIN")
                        enviar_respuesta_cliente(cliente_id, "FIN")
                        del servicios_en_curso[taxi_id]  # Eliminar el servicio completado

# Función para enviar la respuesta al cliente
def enviar_respuesta_cliente(cliente_id, respuesta):
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)    
    print(f"Enviando respuesta {respuesta} al cliente '{cliente_id}'")
    mensaje = f"{cliente_id};{respuesta}"
    producer.send('Central-Customer', value=mensaje.encode('utf-8'))
    producer.flush()

def getPosDestino(destino):
    valor = None
    for pos, valor in dashboard.destinos.items():
        if valor == destino:
            return pos
            break
    if valor is None:
        print(f"No se encontro destino {destino}")
        return (1,1)
    
#####
########## MAIN ##########
#####

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python EC_Central.py <IP_Central> <Puerto_Central>")
        sys.exit(1)

    IP_CENTRAL = sys.argv[1]
    PORT_CENTRAL = int(sys.argv[2])
    print(f"***** [EC_Central] ***** Iniciando con IP: {IP_CENTRAL} y Puerto: {PORT_CENTRAL}")

    # Iniciar el dashboard en el hilo principal
    dashboard = Dashboard()
    # Inicializar el fichero que actuará como base de datos
    inicializar_fichero()
    # Cargar los taxis disponibles al iniciar la central
    cargar_taxis_disponibles()

    # Inicializar el consumidor global de servicios
    inicializar_consumer_servicios()

    # Iniciar el servidor de autenticación por socket en un hilo separado
    hilo_servidor = threading.Thread(target=iniciar_autenticacion_taxis, args=(IP_CENTRAL, PORT_CENTRAL))
    hilo_servidor.daemon = True
    hilo_servidor.start()

    # Hilo para consumir posiciones y estados
    hilo_consumir_posiciones = threading.Thread(target=consumir_posiciones_taxis)
    hilo_consumir_posiciones.daemon = True
    hilo_consumir_posiciones.start()
    
    # Iniciar hilo para procesar mensajes de servicios
    hilo_procesar_servicios = threading.Thread(target=procesar_mensajes_servicios)
    hilo_procesar_servicios.daemon = True
    hilo_procesar_servicios.start()

    # Hilo para peticiones clientes
    hilo_consumir_posiciones = threading.Thread(target=consumir_solicitudes_clientes)
    hilo_consumir_posiciones.daemon = True
    hilo_consumir_posiciones.start()


    dashboard.after(1000, actualizar_dashboard, dashboard)
    dashboard.mainloop()
