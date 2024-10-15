import socket
import time
import sys

def calcular_lrc(estado):
    lrc = 0
    for byte in estado.encode():
        lrc ^= byte
    return lrc

def empaquetar_mensaje(data):
    stx = "<STX>"
    etx = "<ETX>"
    estado = f"{stx}{data}{etx}"
    lrc = calcular_lrc(estado)
    estado_completo = f"{estado}{lrc}"
    return estado_completo

#funcion para enviar mensaje a taxi usando socket
def enviar_estado(taxi_socket, estado):
    try:
        # Enviar el mensaje empaquetado
        taxi_socket.send(estado.encode())
        # Esperar respuesta (ACK/NACK)
        respuesta = taxi_socket.recv(1024).decode()
        if respuesta == "<ACK>":
            print("estado recibido correctamente.")
        elif respuesta == "<NACK>":
            print("Error en la recepción del estado.")
        return respuesta
    except Exception as e:
        print(f"Error al enviar el estado: {e}")

# Función para manejar incidencias del usuario
def manejar_incidencias(taxi_socket):
    while True:
        incidencia = input("Introduce una incidencia o 'n' para ninguna (OK):\n[p] Alerta de peatón, [s] Semáforo rojo, [t] Pinchazo, [v] Vehículo cercano, [n] Ninguna\n> ")

        # Asignar el estado basado en la incidencia seleccionada
        if incidencia == 'p':
            estado_actual = "Alerta de peatón"
        elif incidencia == 's':
            estado_actual = "Semáforo rojo"
        elif incidencia == 't':
            estado_actual = "Pinchazo"
        elif incidencia == 'v':
            estado_actual = "Vehículo cercano"
        elif incidencia == 'n':
            estado_actual = "OK"
        else:
            print("Incidencia no válida. Manteniendo estado anterior.")
            estado_actual = "OK"
        
        # Empaquetar y enviar el estado al taxi
        data = f"{estado_actual}" 
        estado = empaquetar_mensaje(data)
        respuesta = enviar_estado(taxi_socket, estado)
        if respuesta != "<ACK>":
            print("Error en la transmisión. Reintentando...")
        time.sleep(2)

# Iniciar el cliente (EC_S) para enviar los estados del sensor
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python EC_S.py <IP_EC_DE> <PORT_EC_DE>")
        sys.exit(1)

    IP_EC_DE = sys.argv[1]  # IP del taxi (EC_DE)
    PORT_EC_DE = int(sys.argv[2])  # Puerto del taxi (EC_DE)

    try:
        # Crear socket para conectarse al taxi (EC_DE)
        taxi_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        taxi_socket.connect((IP_EC_DE, PORT_EC_DE))
        print(f"Conectado al taxi en {IP_EC_DE}:{PORT_EC_DE}")
        
        # Enviar mensaje de <ENQ> para iniciar la comunicación
        enq = "<ENQ>"
        taxi_socket.send(enq.encode())
        ack = taxi_socket.recv(1024).decode()
        if ack == "<ACK>":
            print("Conexión aceptada. Comunicación iniciada.")
            manejar_incidencias(taxi_socket)
        else:
            print("Conexión rechazada. Cerrando.")

    except Exception as e:
        print(f"Error al conectar con el taxi: {e}")
        print(f"Detalles del error: {e}")
    finally:
        # Finalizar la comunicación enviando <EOT>
        try:
            eot = "<EOT>"
            taxi_socket.send(eot.encode())  # Enviar <EOT> antes de cerrar la conexión
        except Exception as e:
            print(f"Error al enviar <EOT>: {e}")
        taxi_socket.close()