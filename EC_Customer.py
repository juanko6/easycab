import sys
import time
import json
from kafka import KafkaProducer, KafkaConsumer
import configuracion

BOOTSTRAP_SERVER = configuracion.Entorno()
FICHERO_SOLICITUDES = "Requests/EC_Requests.json"
#FICHERO_SOLICITUDES = "SolicitudesClientes/prueba"

class Customer ():
    def __init__(self, ID, bootstrap):
        self.ID = ID
        self.FicheroDetinos = f"{ID}_{FICHERO_SOLICITUDES}"
        
        # Crear Kafka Producer para enviar solicitudes
        print("Iniciando productor Kafka")
        self.producer = KafkaProducer(bootstrap_servers=bootstrap)
        
        # Crear Kafka Consumer para recibir respuestas
        print("Iniciando consumidor Kafka")
        self.consumer = KafkaConsumer('Central-Customer',bootstrap_servers=bootstrap, auto_offset_reset='earliest')

    def start(self):        
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
            print(f"{self.ID}: Esperando nueva solicitud.")
            time.sleep(4)
        
        print(f"{self.ID}: Ya no tengo más destinos. Bye bye.")

    def recibir_respuesta(self):
        print("Esperando respuesta de Central...")
        for message in self.consumer:
            mensaje = message.value.decode('utf-8')
            print(f"mensajes recibidos {mensaje}")
            cliente_ID, respuesta = mensaje.split(";")
            if cliente_ID == self.ID:
                print(f"Respuesta de la central para el cliente '{self.ID}': {respuesta}")
                # Si recibimos "OK" o "KO", la solicitud ha sido procesada
                if respuesta == "OK":
                    print(f"Servicio completado.")
                    break
                elif respuesta == "KO":
                    print(f"Servicio anulado.")
                    break

########## MAIN ##########

if len(sys.argv) != 2:
    print("Uso: python EC_Customer.py <ClienteID>")
    sys.exit(1)

#broker_ip = sys.argv[1]
#broker_puerto = sys.argv[2]
cliente_id = sys.argv[1]

#bootstrap = f'{broker_ip}:{broker_puerto}'
print(f"Crear cliente '{cliente_id}'")
cliente = Customer(cliente_id, BOOTSTRAP_SERVER)
cliente.start()
