import sys
import time
import json
from kafka import KafkaProducer, KafkaConsumer
import configuracion

BOOTSTRAP_SERVER = configuracion.Entorno()
FICHERO_SOLICITUDES = "EC_Requests/Requests.json"
LIMPIAR = False

class Customer ():
    def __init__(self, ID, bootstrap, solicitudes):
        self.ID = ID
        self.FicheroDetinos = f"{solicitudes}"
        
        # Crear Kafka Producer para enviar solicitudes
        print("Iniciando productor Kafka")
        self.producer = KafkaProducer(bootstrap_servers=bootstrap)
        
        # Crear Kafka Consumer para recibir respuestas
        print("Iniciando consumidor Kafka")
        self.consumer = KafkaConsumer('Central-Customer',
                                      bootstrap_servers=bootstrap, 
                                      auto_offset_reset='earliest',
                                      enable_auto_commit=True,
                                      group_id=f"group_{self.ID}")

    def start(self):        
        print(f"Iniciando cliente '{cliente_id}'")
        if not LIMPIAR:      
            print(f"Abrir fichero solicitudes: {self.FicheroDetinos}")
            with open(self.FicheroDetinos , "r") as file:
                data = json.load(file)

            requests = data["Requests"]

            for request in requests:
                destino = request['Id']
                solicitud = f"{self.ID};{destino}"
                
                print(f"{self.ID}: Solictando Taxi a Central para destino {destino}")
                self.producer.send(topic='Customer-Central', value=solicitud.encode('utf-8'))
                self.producer.flush()  # Asegurar que el mensaje se envíe inmediatamente

                # Esperar la respuesta para la solicitud automática
                self.recibir_respuesta()
                print(f"{self.ID}: Esperando para nueva solicitud.")
                time.sleep(4)
            
            print(f"{self.ID}: Ya no tengo más destinos. Bye bye.")
        else:
            self.recibir_respuesta()

    def recibir_respuesta(self):
        print("Esperando respuesta de Central...")
        for message in self.consumer:
            mensaje = message.value.decode('utf-8')
            print(f"mensajes recibidos {mensaje}")
            cliente_ID, respuesta = mensaje.split(";")
            if cliente_ID == self.ID and not LIMPIAR:
                print(f"Respuesta de la central para el cliente '{self.ID}': {respuesta}")
                # Si recibimos "OK" o "KO", la solicitud ha sido procesada
                if respuesta == "OK":
                    print(f"Servicio aceptado.") #Esperamos a que se complete.
                elif respuesta == "KO":
                    print(f"Servicio anulado.")                
                    break   #Salimos para pedir otro servicio.
                elif respuesta == "FIN":
                    print(f"Servicio completado.")
                    break  #Salimos para pedir otro servicio.
            else:
                print(f"{self.ID}:No es para mi lo obvio")

########## MAIN ##########

if len(sys.argv) != 3 :
    print("Uso: python EC_Customer.py <ClienteID> <Fichero Requests>" )
    sys.exit(1)

ficheroSolicitudes = None

cliente_id = sys.argv[1]
if sys.argv[2] == "flush":
    print("Modo: LIMPIAR BUFFER.")
    LIMPIAR = True
else:
    ficheroSolicitudes = sys.argv[2]

#bootstrap = f'{broker_ip}:{broker_puerto}'
cliente = Customer(cliente_id, BOOTSTRAP_SERVER, ficheroSolicitudes)
cliente.start()
