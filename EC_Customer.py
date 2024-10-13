import sys
import time
from kafka import KafkaProducer, KafkaConsumer

FICHERO_SOLICITUDES = "SolicitudesClientes/ListaDestinos"
#FICHERO_SOLICITUDES = "SolicitudesClientes/prueba"

class Customer ():
    def __init__(self, ID, bootstrap):
        self.ID = ID
        self.FicheroDetinos = f"{FICHERO_SOLICITUDES}_{ID}.txt"
        
        # Crear Kafka Producer para enviar solicitudes
        self.producer = KafkaProducer(bootstrap_servers=bootstrap)
        
        # Crear Kafka Consumer para recibir respuestas
        self.consumer = KafkaConsumer('Central-Customer',bootstrap_servers=bootstrap, auto_offset_reset='earliest')

    def start(self):
        with open(self.FicheroDetinos , "r") as file:
            print(f"Abrir fichero: {self.FicheroDetinos}")
            for line in file:
                destino = line
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
        print("esperando respuesta de Central")
        for message in self.consumer:
            print("mensajes recibidos")
            mensaje = message.value.decode('utf-8')
            print(f"mensajes recibidos {mensaje}")
            cliente_ID, respuesta = mensaje.split(";")
            if cliente_ID == self.ID:
                print(f"Respuesta de la central para el cliente {self.ID}: {respuesta}")
                # Si recibimos "OK" o "KO", la solicitud ha sido procesada
                if respuesta == "OK":
                    print(f"Servicio completado.")
                    break
                elif respuesta == "KO":
                    print(f"Servicio anulado.")
                    break

########## MAIN ##########

if len(sys.argv) != 4:
    print("Uso: python EC_Customer.py <BrokerIP> <BrokerPuerto> <ClienteID>")
    sys.exit(1)
#TODO: Es necesario pasar la IP del broker, esto lo podemos coger equipo.config
broker_ip = sys.argv[1]
broker_puerto = sys.argv[2]
cliente_id = sys.argv[3]

bootstrap = f'{broker_ip}:{broker_puerto}'

cliente = Customer(cliente_id, bootstrap)
cliente.start()
