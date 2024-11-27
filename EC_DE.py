import signal
import sys
import socket
import threading
from kafka import KafkaProducer,KafkaConsumer
import time
import random
import configuracion

BOOTSTRAP_SERVER = configuracion.Entorno()
HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"
MAPA_DIM = 20  # Dimensión del mapa (20x20)

class EC_DE:
    def __init__(self, ID, bootstrap):        
        print(f"***** [EC_DE] ***** Iniciando Taxi ID: {ID} con Kafka en {bootstrap}")
        self.ID = ID
        self.estado = "esperandoconexion"  # Estado inicial del taxi
        self.posicion = [0, 0]  # Inicializar la posición del taxi en (1,1)
        self.topic = f"TAXI_{self.ID}"
        self.sensor_conectado = False  # Estado de conexión del sensor

        # Crear Kafka Producer para enviar actulizaciones de estado y posición
        print("Iniciando productor Kafka")
        self.producer = KafkaProducer(bootstrap_servers=bootstrap)
        
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
            elif result == 2:
                print("Taxi con ID ya estaba registrado.")
            elif result == -1:
                print("ID fuera de rango. Solo permitido [0..99].")
            elif result == -3:
                print("Mensaje de autenticación incorrecto.")
            else:
                print("Error desconocido en la autenticación del taxi.")

            client.close()
        except Exception as e:
            print(f"Error al conectar con la central: {e}")

        return result

    def escuchar_asignaciones(self):
        """Escuchar asignaciones de servicios y procesarlas"""
        consumer = KafkaConsumer(
            f"TAXI_{self.ID}",
            bootstrap_servers=BOOTSTRAP_SERVER,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=f'group_{self.ID}'
        )
        print(f"[EC_DE] Taxi {self.ID} escuchando asignaciones en topic TAXI_{self.ID}")

        for mensaje in consumer:
            servicio = mensaje.value.decode('utf-8')
            if ";" in servicio:
                ubicacion_cliente, destino = servicio.split(';')
                print(f"[EC_DE] Taxi {self.ID} recibió servicio: Ubicación Cliente {ubicacion_cliente}, Destino {destino}")
                self.recibir_servicio(ubicacion_cliente, destino)

    def recibir_servicio(self, ubicacion_cliente, destino):
        """Mover el taxi hacia la ubicación del cliente y luego al destino"""
        # Extraer coordenadas del cliente y destino
        ubicacion_cliente_x, ubicacion_cliente_y = map(int, ubicacion_cliente.strip('()').split(','))
        destino_x, destino_y = map(int, destino.strip('()').split(','))

        self.estado = "en camino"
        # Mover hacia el cliente
        self.mover_hacia(ubicacion_cliente_x, ubicacion_cliente_y)
        print("[Servicion] enviar Parte 1")
        time.sleep(1)
        self.enviar_estado_servicio("1") #1=Llegada al cliente

        self.estado = "en servicio"
        # Cambiar el estado a "en servicio" y moverse al destino
        self.mover_hacia(destino_x, destino_y)
        print("[Servicio] enviar Parte 2")
        self.enviar_estado_servicio("2") #2=Finalizado

        # Finalizar el servicio
        self.estado = "Disponible"
        print(f"[EC_DE] Servicio finalizado. Taxi {self.ID} ahora está disponible.")

    def mover_hacia(self, dest_x, dest_y):
        #Mover el taxi paso a paso hacia una posición objetivo.
        print(f"[EC_DE] Taxi {self.ID} en estado: {self.estado}. Moviéndose hacia ({dest_x}, {dest_y})")

        while self.posicion != [dest_x, dest_y]:
            if self.estado == "en camino" or self.estado == "en servicio":     #Solo mover si se ha activado.
                # Movimiento en el eje X
                if self.posicion[0] < dest_x:
                    self.posicion[0] += 1
                elif self.posicion[0] > dest_x:
                    self.posicion[0] -= 1

                time.sleep(1)

                # Movimiento en el eje Y
                if self.posicion[1] < dest_y:
                    self.posicion[1] += 1
                elif self.posicion[1] > dest_y:
                    self.posicion[1] -= 1

                time.sleep(1)

        print(f"[EC_DE] Taxi {self.ID} ha llegado a la posición ({dest_x}, {dest_y})")

    def enviar_estado_servicio(self, mensaje):
        #Enviar la posición y el estado actual a Kafka.
        self.producer.send(f"ST_{self.ID}", value=mensaje.encode('utf-8'))
        self.producer.flush()
        print(f'Enviando SERVICIO: {mensaje}')

    
    # Función que actualiza el estado del taxi basado en el sensor
    def actualizar_estado(self):
        while self.running:
            if not self.sensor_conectado:
                self.estado = "esperandoconexion"
                print(f"Taxi {self.ID} en espera de conexión con el sensor.")
            else:
                estado_sensor = leer_estado_sensor(self.ID)
                print(f"Taxi {self.ID} sensor conectado con estado {estado_sensor}.")
                if estado_sensor == "OK":
                # Solo cambiar a "Disponible" si no está en movimiento o en servicio
                    if self.estado not in ["en camino", "en servicio"]:
                        self.estado = "Disponible"
                elif estado_sensor != "OK":
                # Cambiar a "KO" solo si el sensor indica un problema
                    self.estado = "KO"
                    
            enviar_posicion_estado_kafka(self.ID, self.posicion, self.estado, self.producer, self.topic)
            time.sleep(1)

    def escuchar_comandos(self):
        """
        Escucha comandos en el topic del taxi y los procesa.
        """
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=BOOTSTRAP_SERVER,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=f'group_{self.ID}'
        )
        print(f"[INFO] Taxi {self.ID} escuchando comandos en {self.topic}")
        for mensaje in consumer:
            comando = mensaje.value.decode('utf-8')
            print(f"[INFO] Taxi {self.ID} recibió comando: {comando}")
            if comando.startswith("MOVER:"):
                _, coords = comando.split(":")
                x, y = map(int, coords.split(","))
                self.mover_hacia(x, y)

    def mover_hacia(self, dest_x, dest_y):
        """
        Mueve el taxi paso a paso hacia la posición objetivo.
        """
        print(f"[INFO] Taxi {self.ID} moviéndose hacia ({dest_x}, {dest_y})")
        while self.posicion != [dest_x, dest_y]:
            if self.posicion[0] < dest_x:
                self.posicion[0] += 1
            elif self.posicion[0] > dest_x:
                self.posicion[0] -= 1
            time.sleep(1)

            if self.posicion[1] < dest_y:
                self.posicion[1] += 1
            elif self.posicion[1] > dest_y:
                self.posicion[1] -= 1
            time.sleep(1)

        print(f"[INFO] Taxi {self.ID} llegó a la posición ({dest_x}, {dest_y})")
        self.estado = "Disponible"


    # Función para detener el taxi de forma ordenada
    def detener(self):
        print("\n[EC_DE] Apagando el taxi...")
        self.running = False

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
    estadoAnterior = "Disponible"
    while True:
        try:
            # Recibir estado del sensor
            estado_sensor = conn_sensor.recv(4096).decode(FORMAT)
            if not estado_sensor:
                print("Sensor desconectado.")
                taxi.estado = "esperandoconexion"
                taxi.sensor_conectado = False
                enviar_posicion_estado_kafka(taxi.ID, taxi.posicion, taxi.estado, taxi.producer, taxi.topic)
                break

            if estado_sensor == "<EOT>":
                print("Sensor envió <EOT>. Finalizando comunicación.")
                taxi.estado = "esperandoconexion"
                taxi.sensor_conectado = False
                enviar_posicion_estado_kafka(taxi.ID, taxi.posicion, taxi.estado, taxi.producer, taxi.topic)
                break

            print(f"Estado recibido del sensor: {estado_sensor}")
            taxi.sensor_conectado = True

            data, lrc_recibido = desempaquetar_estado(estado_sensor)
            if data is None:
                print("Error en el formato del mensaje")
                conn_sensor.send("<NACK>".encode())
                continue

            if verificar_lrc(estado_sensor[:estado_sensor.find("<ETX>")+len("<ETX>")], lrc_recibido):
                print(f"Estado válido: {data}")
                conn_sensor.send("<ACK>".encode())

                # Cambiar el estado del taxi en función del mensaje recibido
                if data == "OK":
                    print(estadoAnterior)
                    taxi.estado = estadoAnterior
                else:
                    if taxi.estado == "Disponible" or taxi.estado == "en camino" or taxi.estado ==  "en servicio":
                        estadoAnterior = taxi.estado
                    taxi.estado = "KO"
                    print(f"Incidencia detectada: {data}")

                # Confirmar y enviar el estado actualizado a EC_Central
                print(f"Taxi {taxi.ID}: Posicion {taxi.posicion}, Estado {taxi.estado}")
                enviar_posicion_estado_kafka(taxi.ID, taxi.posicion, taxi.estado, taxi.producer, taxi.topic)

                
                # Guardar el estado en un fichero
                fichero_estado = f"estado_sensor_taxi_{taxi.ID}.txt"
                with open(fichero_estado, "w") as file:
                    file.write(f"{data}\n")
            else:
                print("Error: LRC no coincide. El mensaje está corrupto.")
                conn_sensor.send("<NACK>".encode())

        except socket.timeout:
            print("Timeout: No se recibió un mensaje del sensor.")
            taxi.estado = "esperandoconexion"
            taxi.sensor_conectado = False
            enviar_posicion_estado_kafka(taxi.ID, taxi.posicion, taxi.estado, taxi.producer, taxi.topic)
            break

        except ConnectionResetError:
            print("Conexión perdida con el sensor.")
            taxi.estado = "esperandoconexion"
            taxi.sensor_conectado = False
            enviar_posicion_estado_kafka(taxi.ID, taxi.posicion, taxi.estado, taxi.producer, taxi.topic)
            break

        except Exception as e:
            print(f"Error al recibir estado del sensor: {e}")
            taxi.estado = "esperandoconexion"
            taxi.sensor_conectado = False
            enviar_posicion_estado_kafka(taxi.ID, taxi.posicion, taxi.estado, taxi.producer, taxi.topic)
            break
        
    conn_sensor.close()
    taxi.sensor_conectado = False
    print("Conexión cerrada con el sensor.")
    print(f"Esperando conexion del sensor en {SENSOR_IP}:{SENSOR_PORT}...")

# Función para enviar la posición y el estado del taxi a Kafka
def enviar_posicion_estado_kafka(taxi_id, posicion, estado, producer, topic):
    mensaje = f"Taxi {taxi_id}: Posicion {posicion}, Estado {estado}"
    producer.send(topic, value=mensaje.encode('utf-8'))
    #print(f"Enviando posición y estado del taxi: {mensaje}")
    producer.flush()  # Asegura que el mensaje se envía inmediatamente

# Función para leer el estado del sensor desde el archivo identificado por el ID del taxi
def leer_estado_sensor(taxi_id):
    fichero_estado = f"estado_sensor_taxi_{taxi_id}.txt"
    try:
        with open(fichero_estado, "r") as file:
            return file.read().strip()  # Leer y devolver el estado sin espacios en blanco
    except FileNotFoundError:
        return "OK"  # Si no existe el archivo, retornar "OK" por defecto
    except Exception as e:
        print(f"Error al leer el estado del sensor para Taxi {taxi_id}: {e}")
        return "OK"  # Manejar otros errores devolviendo "OK" por defecto


# Función para enviar mensajes al servidor central
def send(client, msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)


# Función para aceptar conexiones y manejar las desconexiones de EC_S
def aceptar_conexiones_S(server_socket):
    while taxi.running:  # Verificar que taxi siga corriendo
        try:
            print("Esperando conexion del sensor...")
            conn_sensor, addr = server_socket.accept()
            taxi.sensor_conectado = True  # Marcar sensor como conectado
            taxi.estado = "Disponible"  # Cambiar estado a disponible si el sensor está conectado y sin incidencias
            taxi.running = True
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
            taxi.sensor_conectado = False  # Marcar sensor como desconectado
            taxi.estado = "esperandoconexion"  # Volver al estado de espera
            taxi.running = False  # Detener el taxi en caso de desconexión

########## MAIN ##########

def signal_handler(sig, frame):
    print("\n[EC_DE] Señal de interrupción recibida. Finalizando...")
    taxi.detener()  # Detener el taxi y sus hilos
    if server_socket:
        server_socket.close()  # Cerrar el socket del servidor para desbloquear el accept()
    sys.exit(0)  # Terminar el programa

if len(sys.argv) == 6:
    SERVER_CENTRAL = sys.argv[1]
    PORT_CENTRAL = int(sys.argv[2])
    ADDR_CENTRAL = (SERVER_CENTRAL, PORT_CENTRAL)
    ID = int(sys.argv[3])

    SENSOR_IP = sys.argv[4]  # IP del sensor (EC_S)
    SENSOR_PORT = int(sys.argv[5])  # Puerto del sensor (EC_S)    

    # Crear el taxi
    taxi = EC_DE(ID, BOOTSTRAP_SERVER)

    # Si la autenticación con la central es exitosa, iniciar el proceso de estados y sensor
    if taxi.conectar_central(ADDR_CENTRAL) > 0:
        # Iniciar el manejo de señales para cerrar el taxi con Ctrl+C
        signal.signal(signal.SIGINT, signal_handler)

        # Iniciar la actualización del estado del taxi
        hilo_estado = threading.Thread(target=taxi.actualizar_estado, daemon=True)
        hilo_estado.start()

        print(f"Esperando conexion del sensor en {SENSOR_IP}:{SENSOR_PORT}...")
        # Iniciar el servidor para recibir estados de los sensores en un hilo separado
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((SENSOR_IP, SENSOR_PORT))
        server_socket.listen(1)

        # Crear el hilo para aceptar conexiones
        hilo_sensor = threading.Thread(target=aceptar_conexiones_S, args=(server_socket,), daemon=True)
        hilo_sensor.daemon = True
        hilo_sensor.start()

        # Crear hilo para escuchar asignaciones de servicio de la central
        hilo_escuchar_asignaciones = threading.Thread(target=taxi.escuchar_asignaciones, daemon=True)
        hilo_escuchar_asignaciones.start()

        # Mantener el proceso activo
        while True:
            time.sleep(1)

else:
    print("Oops!. Parece que algo falló. Necesito estos argumentos: <ServerIP> <Puerto> <ID> <PuertoSensor> <IPSensor>")
