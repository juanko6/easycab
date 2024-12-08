import socket
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import threading
import time
import sys
import os
import configuracion
import subprocess
import json
import miSQL
from flask import Flask, request, jsonify, render_template

URL_EC_CTC = "http://192.168.1.141:5001/traffic"  # Cambia la IP si EC_CTC está en otro servidor
CIUDAD_SERVICIO = "Alicante"  # Ciudad donde opera el servicio

# Intervalo de consulta a EC_CTC (en segundos)
INTERVALO_CONSULTA = 10



BOOTSTRAP_SERVER = configuracion.Entorno()
#GROUP_ID = 'central-group'
REFRESH_INTERVAL = 2  # Intervalo en segundos para verificar nuevos topics
HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"
DB_TAXIS = "taxis_db.txt"  # Fichero que actuará como base de datos
DB_CUSTOMERS = "customer_db.txt"  # Fichero que actuará como base de datos para customer

MAX_TAXIS = configuracion.LicenciasTaxis()

servicios_en_curso = {}  # Para trackear servicios activos
consumer_servicios = None  # Consumer global para servicios

# Lista de taxis disponibles y taxis autenticados
taxis_autenticados = set()
newTopicsTaxis = set()
taxis_disponibles = set()
taxi_ids = {}  # Taxi_ID : Conexión
nuevos_estados = {} 

#####
########## PERSITENCIA DATOS ##########
#####
def guardar_cliente_SQL(cliente_id, destino, estado, posicion):
    return sql.insertOrUpdateCliente(cliente_id, destino[0], destino[1], estado, posicion[0], posicion[1])

# Función para guradar las posiciones y estados de los taxis 
def guardar_taxi_SQL(taxi_id, posicion, estado, conectado):
    posY, posX  = posicion
    sql.UpdateTAXI(taxi_id, posX, posY, estado, conectado)

#Funcion para iniciar los taxis desde la base de datos.
def cargar_taxis_SQL():
    result = sql.consulta("SELECT ID_TAXI, POS_X, POS_Y FROM TAXI;")
    print("----------LIST TAXIS REGISTRADOS-------------")
    for registro in result:
        taxi_id, pos_X, pos_Y = registro
        estado = 'SinConexion'
        conectado = False
        posicion = [pos_Y,pos_X]
        print(f"{taxi_id}: ({pos_X},{pos_Y}); {estado}")
        guardar_taxi_SQL(taxi_id, posicion, estado, conectado)  # Actualizamos estado en SQL


#####
########## AUTENTICACION TAXIS ##########
#####

def nuevo_taxi(conn, addr):
    print(f"[PETICIÓN AUTENTICACIÓN] {addr} connected.")
    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            print(f"Recibido del taxi [{addr}] el mensaje: {msg}")

            # Extraer el ID del taxi del mensaje
            if msg.startswith("Nuevo Taxi"):
                try:
                    taxi_id = int(msg.split()[2])  # Extraer el ID como entero
                except (IndexError, ValueError):
                    conn.send("-3".encode(FORMAT))  # Mensaje malformado
                    connected = False
                    continue

                # Verificar el ID del taxi
                result = autenticar_taxiSQL(taxi_id)

                if result == 1:
                    print(f"Taxi con ID {taxi_id} autenticado correctamente.")
                    conn.send("1".encode(FORMAT))
                elif result == 2:
                    print(f"Ya existe un taxi autenticado con ID {taxi_id}.")
                    conn.send("2".encode(FORMAT))
                elif result == -1:
                    print(f"ID {taxi_id} no registrado.")
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

def autenticar_taxiSQL(idTaxi):
    if sql.checkTaxiId(idTaxi):
        if idTaxi not in taxis_autenticados:  # Verificar si el ID ya ha sido autenticado
            taxis_autenticados.add(idTaxi)
            newTopicsTaxis.add(f"TAXI_{idTaxi}")
            
            return 1  # ID autenticado correctamente
        else:
            return 2  # ID ya autenticado
    else:
        return -1 # ID no registrado

# Función para iniciar el servidor de autenticación
def iniciar_autenticacion_taxis(IP_CENTRAL, PORT_CENTRAL):    
    print("***Inicio hilo autenticación taxis***")
    ADDR = (IP_CENTRAL, PORT_CENTRAL)
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)
    server.listen()
    print(f"[AUTENTICACION] Servidor a la escucha en {ADDR}")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=nuevo_taxi, args=(conn, addr))
        thread.start()

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

#           guardar_en_fichero(taxi_id, posicion, estado)  # Guardar estado en el fichero
            guardar_taxi_SQL(taxi_id, posicion, estado, True)  # Guardar estado en SQL

#####
########## CUSTOMER ##########
#####

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
        cliente_id, destino_id, posicionStr = solicitud.split(";")
        posicion = posicionStr.split(",")
        destino = getPosUbicacion(destino_id)
        print(f"Disponibles:....{taxis_disponibles}")
        if taxis_disponibles:
            taxi_id_disponible = taxis_disponibles.pop() # Obtener taxi y marcar como no disponible
#            iniciar_ubicacion_cliente(cliente_id)
            enviar_respuesta_cliente(cliente_id, "OK")  # Enviar OK servicio aceptado.    
#            guardar_en_fichero_customer(cliente_id, destino, "OK")
            posicion = guardar_cliente_SQL(cliente_id, destino, "OK", posicion)
            asignar_taxi(taxi_id_disponible, destino_id, destino, cliente_id, posicion)                         
        else:
            enviar_respuesta_cliente(cliente_id, "KO")  # Enviar KO si no hay taxis
#            guardar_en_fichero_customer(cliente_id, destino, "KO")
            guardar_cliente_SQL(cliente_id, destino, "KO", posicion)
            print("No hay taxis disponibles en este momento")                  

# Función para asignar un taxi a una solicitud
def asignar_taxi(taxi_id, destino, posDetino, cliente_id, posCliente):
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
    topic_taxi = f"TAXI_{taxi_id}"
    
    # 0 - Guardar cliente asignado en TAXI
    sql.UpdateClienteTAXI(taxi_id, cliente_id)

    # 1 - Registrar el servicio en curso
    servicios_en_curso[taxi_id] = {
        'cliente_id': cliente_id,
        'origen': posCliente,
        'destino': destino,
        'pos_destino': posDetino
    }

    subscribir_a_servicio_taxi(taxi_id)

        #2 - Enviar TAXI a por CLIENTE.
    servicio = f"{posCliente};{posDetino}"
    producer.send(topic_taxi, value=servicio.encode('utf-8'))
    producer.flush()
    print(f"Servicio para {topic_taxi}: {servicio}")

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
                        sql.UpdateEstadoCLIENTE(cliente_id, f'Taxi_{taxi_id}')
                        print(f"Cliente '{cliente_id}' recogido por taxi {taxi_id}")
                    
                    elif contenido == 2:  # taxi confirma llegada al destino
                        cliente_id = servicio['cliente_id']
                        destino = servicio['destino']
                        pos_destino = servicio['pos_destino']
                        print(f"Cliente '{cliente_id}' dejado en destino {destino} por taxi {taxi_id}")
                        taxis_disponibles.add(taxi_id)
                        columna, fila = pos_destino
                        sql.UpdateLlegadaCLIENTE(cliente_id, columna, fila, f'FIN')
                        enviar_respuesta_cliente(cliente_id, f"FIN;{columna},{fila}")
                        del servicios_en_curso[taxi_id]  # Eliminar el servicio completado

# Función para enviar la respuesta al cliente
def enviar_respuesta_cliente(cliente_id, respuesta):
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)    
    print(f"Enviando respuesta {respuesta} al cliente '{cliente_id}'")
    mensaje = f"{cliente_id}|{respuesta}"
    producer.send('Central-Customer', value=mensaje.encode('utf-8'))
    producer.flush()

def getPosUbicacion(destino):
    pos = sql.getPosUbicacion(destino)
    return pos

#####
########## REQUEST CITY TRAFFIC CONTROL ##########
#####
class ECCentral:
    def __init__(self):
        self.estado_trafico = "OK"
#        self.dashboard = dashboard  # Dashboard pasado desde el hilo principal
        self.producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
        self.ciudad = CIUDAD_SERVICIO
        self.app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), "templates"))  # Inicializar Flask
        self.configurar_endpoints()  # Configurar los endpoints REST



    def configurar_endpoints(self):

        # Estructura inicial de los taxis
        #self.dashboard.taxis = {
        #    1: {"posicion": [0, 0], "estado": "Disponible", "cliente_asignado": None},
        #    2: {"posicion": [10, 10], "estado": "Disponible", "cliente_asignado": None}
        #}


        #self.dashboard.clientes = {
        #    1: {"destino": [5, 5], "estado": "Esperando"},
        #    2: {"destino": [10, 15], "estado": "En servicio"}
        #}

        """Define los endpoints del API REST en Flask."""
        @self.app.route('/dashboard')
        def mostrar_dashboard():
            """
            Renderiza el dashboard con los datos actuales de los taxis.
            """
            taxis = self.dashboard.taxis  # Obtiene los datos actuales de los taxis
            clientes = getattr(self.dashboard, 'clientes', {})  # Datos de clientes, vacío si no existe

            return render_template("dashboard.html", taxis=taxis, clientes=clientes)




        @self.app.route('/api/dashboard-data', methods=['GET'])
        def obtener_datos_dashboard():
            """
            Devuelve los datos de taxis y clientes en formato JSON, organizados como diccionarios.
            """
            try:
                # Consulta de datos desde la base de datos
                taxis_resultados = sql.consulta("SELECT ID_TAXI, POS_X, POS_Y, ESTADO, CONECTADO, ID_CLIENTE FROM TAXI") or []
                clientes_resultados = sql.consulta("SELECT ID_CLIENTE, DES_X, DES_Y, ESTADO, POS_X, POS_Y FROM CLIENTE") or []

                # Convertir resultados en diccionarios
                taxis = [
                    {
                        "id": taxi[0],  # ID_TAXI
                        "posicion": [taxi[1], taxi[2]],  # POS_X, POS_Y
                        "estado": taxi[3],  # ESTADO
                        "conectado": bool(taxi[4]),  # CONECTADO
                        "cliente_asociado": taxi[5]  # ID_CLIENTE
                    }
                    for taxi in taxis_resultados
                ]

                clientes = [
                    {
                        "id": cliente[0],  # ID_CLIENTE
                        "destino": [cliente[1], cliente[2]],  # DES_X, DES_Y
                        "estado": cliente[3],  # ESTADO
                        "posicion": [cliente[4], cliente[5]]  # POS_X, POS_Y
                    }
                    for cliente in clientes_resultados
                ]

                # Retornar los datos en formato JSON
                return {
                    "taxis": taxis,
                    "clientes": clientes
                }, 200

            except Exception as e:
                print(f"Error al consultar datos del dashboard: {e}")
                return {"error": "Error al consultar datos del dashboard"}, 500


        @self.app.route('/api/taxis/mover', methods=['POST'])
        def mover_taxi():
            data = request.json
            taxi_id = data.get('taxi_id')
            posicion = data.get('posicion')

            if not taxi_id or not posicion:
                return jsonify({"error": "Debe proporcionar 'taxi_id' y 'posicion'"}), 400

            if taxi_id not in self.dashboard.taxis:
                return jsonify({"error": f"Taxi con ID {taxi_id} no encontrado"}), 404

            self.enviar_comando_taxi(taxi_id, posicion)
            return jsonify({"message": f"Comando enviado al taxi {taxi_id} para moverse a {posicion}"}), 200

        @self.app.route('/api/taxis/base', methods=['POST'])
        def enviar_todos_a_base():
            for taxi_id in self.dashboard.taxis.keys():
                self.enviar_comando_taxi(taxi_id, (1, 1))
            return jsonify({"message": "Todos los taxis han sido enviados a la base"}), 200

        @self.app.route('/api/ciudad', methods=['POST'])
        def cambiar_ciudad():
            """
            Cambia la ciudad global CIUDAD_SERVICIO y actualiza el estado del tráfico.
            """
            global CIUDAD_SERVICIO  # Acceder a la variable global
            # Intentar obtener datos en formato JSON
            if request.is_json:
                data = request.get_json()
                nueva_ciudad = data.get('ciudad')
            else:
                # Intentar obtener datos en formato de formulario
                nueva_ciudad = request.form.get('ciudad')

            if not nueva_ciudad:
                return jsonify({"error": "Debe proporcionar el nombre de la ciudad"}), 400

            # Cambiar la ciudad global y consultar tráfico
            CIUDAD_SERVICIO = nueva_ciudad
            self.estado_trafico = self.consultar_trafico()
            return jsonify({"message": f"Ciudad cambiada a {CIUDAD_SERVICIO}", "trafico": self.estado_trafico}), 200


    def consultar_trafico(self):
        """Consulta el estado del tráfico en EC_CTC usando curl."""
        try:
            url = f"{URL_EC_CTC}?ciudad={CIUDAD_SERVICIO}"
            result = subprocess.run(
                ["curl", "-s", url],
                capture_output=True,
                text=True
                
            )

            if result.returncode != 0:
                print(f"[ERROR] No se pudo conectar con EC_CTC: {result.stderr}")
                return

            # Parsear la respuesta JSON
            datos = json.loads(result.stdout)
            self.estado_trafico = datos.get("estado_trafico", "KO")
            print(f"[INFO] Estado del tráfico: {self.estado_trafico}")
        except Exception as e:
            print(f"[ERROR] Al consultar EC_CTC: {e}")
            self.estado_trafico = "KO"

    def verificar_trafico_periodicamente(self):
        """Consulta el tráfico periódicamente y actualiza el estado en el sistema."""
        while True:
            self.consultar_trafico()
            if self.estado_trafico == "KO":
                print("[ALERTA] Tráfico no viable. Notificando a los taxis.")
                self.enviar_todos_a_base()
            time.sleep(INTERVALO_CONSULTA)

    def enviar_comando_taxi(self, taxi_id, posicion):
        """Envía un comando al taxi para moverse a una posición específica."""
        topic_taxi = f"TAXI_{taxi_id}"
        mensaje = f"MOVER:{posicion[0]},{posicion[1]}"
        self.producer.send(topic_taxi, value=mensaje.encode('utf-8'))
        self.producer.flush()
        print(f"[INFO] Comando enviado a {topic_taxi}: {mensaje}")

    def enviar_todos_a_base(self):
        """
        Envía un comando a todos los taxis para regresar a su posición base (1,1).
        """
        taxis_disponibles = self.dashboard.taxis.keys()  # IDs de los taxis registrados
        for taxi_id in taxis_disponibles:
            self.enviar_comando_taxi(taxi_id, (1, 1))
        print("[INFO] Todos los taxis enviados a la base.")
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

    # Inicializar base de datos
    sql = miSQL.MiSQL()
    # Cargar los taxis al iniciar la central
    cargar_taxis_SQL()

    # Inicializar el consumidor global de servicios
    inicializar_consumer_servicios()

    # Iniciar el servidor de autenticación por socket en un hilo separado
    hilo_servidor = threading.Thread(target=iniciar_autenticacion_taxis, args=(IP_CENTRAL, PORT_CENTRAL))
    hilo_servidor.daemon = True
    hilo_servidor.start()

    # Iniciar la verificación periódica del tráfico en un hilo
    central = ECCentral()
    hilo_trafico = threading.Thread(target=central.verificar_trafico_periodicamente, daemon=True)
    hilo_trafico.start()

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


    central.app.run(host="0.0.0.0", port=5000, debug=False)
