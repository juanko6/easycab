from kafka import KafkaProducer
import sys

# Configuración del Kafka Producer
def crear_producer(broker_ip, broker_puerto):
    return KafkaProducer(bootstrap_servers=f'{broker_ip}:{broker_puerto}')

def enviar_solicitud(cliente_id, destino, producer):
    mensaje = f"Cliente {cliente_id}: Solicito un taxi hacia {destino}"
    producer.send('solicitudes-clientes', value=mensaje.encode('utf-8'))
    print(f"Solicitud enviada: {mensaje}")
    producer.flush() # Asegura que los mensajes se envían de inmediato

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Uso: python cliente.py <BrokerIP> <BrokerPuerto> <ClienteID> <Destino>")
        sys.exit(1)

    broker_ip = sys.argv[1]
    broker_puerto = sys.argv[2]
    cliente_id = sys.argv[3]
    destino = sys.argv[4]

    # Crear Kafka Producer usando los argumentos
    producer = crear_producer(broker_ip, broker_puerto)

    # Enviar la solicitud del cliente
    enviar_solicitud(cliente_id, destino, producer)
