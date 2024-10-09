import sys
import time
from kafka import KafkaProducer, KafkaConsumer

FICHERO_SOLICITUDES = "clientes_solicitudes.txt"

# Función para enviar solicitud inicial
def enviar_solicitud(cliente_id, solicitud, producer):
    # Escribir la solicitud en el fichero
    with open(FICHERO_SOLICITUDES, "a") as file:
        file.write(f"{cliente_id}: {solicitud}\n")
    print(f"Solicitud enviada: {solicitud}")

    # Enviar la solicitud al topic de la central
    producer.send('solicitudes-central', value=solicitud.encode('utf-8'))
    producer.flush()  # Asegurar que el mensaje se envíe inmediatamente

# Función para recibir la respuesta de la central
def recibir_respuesta(cliente_id, consumer):
    for message in consumer:
        respuesta = message.value.decode('utf-8')
        print(f"Respuesta de la central para el cliente {cliente_id}: {respuesta}")
        
        # Si recibimos "OK" o "KO", la solicitud ha sido procesada
        if respuesta == "OK":
            print(f"Cliente {cliente_id}: Servicio aceptado.")
            break
        elif respuesta == "KO":
            print(f"Cliente {cliente_id}: No hay taxis disponibles.")
            break

########## MAIN ##########

if len(sys.argv) != 4:
    print("Uso: python EC_Customer.py <BrokerIP> <BrokerPuerto> <ClienteID>")
    sys.exit(1)

broker_ip = sys.argv[1]
broker_puerto = sys.argv[2]
cliente_id = sys.argv[3]

# Crear Kafka Producer para enviar solicitudes
producer = KafkaProducer(bootstrap_servers=f'{broker_ip}:{broker_puerto}')

# Crear Kafka Consumer para recibir respuestas
consumer = KafkaConsumer(
    f'cliente_{cliente_id}',
    bootstrap_servers=f'{broker_ip}:{broker_puerto}',
    auto_offset_reset='earliest',
    group_id=f'cliente_{cliente_id}_group'
)

# Solicitud inicial manual
solicitud_inicial = input("Ingrese su primera solicitud (ej: Solicito taxi en Calle 10): ")
enviar_solicitud(cliente_id, solicitud_inicial, producer)

# Esperar la respuesta de la central para la primera solicitud
recibir_respuesta(cliente_id, consumer)

# Automatizar las siguientes solicitudes
while True:
    solicitud_automatica = f"Solicitud automática del cliente {cliente_id}"
    enviar_solicitud(cliente_id, solicitud_automatica, producer)
    
    # Esperar la respuesta para la solicitud automática
    recibir_respuesta(cliente_id, consumer)
    
    time.sleep(10)  # Realizar solicitudes automáticas cada 10 segundos
