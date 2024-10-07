import socket
import sys
import time
import threading

# Función para enviar mensajes de estado cada segundo en segundo plano
def enviar_mensaje_ok(sensor_socket):
    while True:
        message = "OK"
        sensor_socket.sendall(message.encode())
        time.sleep(1)  # Enviar cada segundo en segundo plano

# Función para leer incidencias del usuario y enviarlas
def enviar_incidencias(sensor_socket):
    while True:
        # Pedir al usuario una incidencia
        print("Introduce una incidencia o 'n' para ninguna (OK):")
        print("[p] Alerta de peatón, [s] Semáforo rojo, [t] Pinchazo, [v] Vehículo cercano, [n] Ninguna")
        incidencia = input("> ")

        # Definir el tipo de incidencia
        if incidencia == 'p':
            message = "Alerta de peatón"
        elif incidencia == 's':
            message = "Semáforo rojo"
        elif incidencia == 't':
            message = "Pinchazo"
        elif incidencia == 'v':
            message = "Vehículo cercano"
        elif incidencia == 'n':
            message = "OK"
        else:
            print("Incidencia no válida, enviando OK por defecto.")
            message = "OK"

        # Enviar el mensaje de incidencia o "OK"
        sensor_socket.sendall(message.encode())
        print(f"Enviando incidencia: {message}")

# Ejecutar el sensor (EC_S)
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python EC_S.py <IP_DE> <PUERTO_DE>")
        sys.exit(1)

    IP_DIGITAL_ENGINE = sys.argv[1]  # IP del EC_DE
    PORT_DIGITAL_ENGINE = int(sys.argv[2])  # Puerto del EC_DE

    # Crear la conexión con el Digital Engine
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sensor_socket:
        sensor_socket.connect((IP_DIGITAL_ENGINE, PORT_DIGITAL_ENGINE))
        print(f"Conectado a EC_DE en {IP_DIGITAL_ENGINE}:{PORT_DIGITAL_ENGINE}")

        # Iniciar el envío de mensajes OK en segundo plano
        hilo_ok = threading.Thread(target=enviar_mensaje_ok, args=(sensor_socket,))
        hilo_ok.daemon = True  # Este hilo se detendrá cuando el programa termine
        hilo_ok.start()

        # Iniciar la escucha de incidencias del usuario
        enviar_incidencias(sensor_socket)
