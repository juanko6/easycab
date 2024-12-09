import socket
import json

def conectar_servidor(host='127.0.0.1', puerto=12345):
    cliente_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    cliente_socket.connect((host, puerto))
    return cliente_socket

def autenticar(cliente_socket, id_usuario, password):
    # Enviar las credenciales de autenticación
    datos = {'id_usuario': id_usuario, 'password': password}
    cliente_socket.send(json.dumps(datos).encode('utf-8'))

    # Recibir la respuesta del servidor
    respuesta = cliente_socket.recv(1024).decode('utf-8')
    respuesta = json.loads(respuesta)
    
    if 'token' in respuesta:
        print(f"Autenticación exitosa. Token recibido: {respuesta['token']}")
        return respuesta['token']
    else:
        print(f"Error de autenticación: {respuesta['mensaje']}")
        return None

def comunicarse_con_servidor(cliente_socket, token):
    # Enviar el token para realizar una solicitud autenticada
    datos = {'token': token}
    cliente_socket.send(json.dumps(datos).encode('utf-8'))

    # Recibir la respuesta del servidor
    respuesta = cliente_socket.recv(1024).decode('utf-8')
    respuesta = json.loads(respuesta)
    print(respuesta['mensaje'])

#####
##TODO:Sustituir main por llamadas reales desde EC_DE.py
def main(): 
    id_usuario = input("Introduce tu ID de usuario: ")
    password = input("Introduce tu contraseña: ")

    cliente_socket = conectar_servidor()
    
    # Autenticación
    token = autenticar(cliente_socket, id_usuario, password)
    
    if token:
        # Comunicación posterior con token
        comunicarse_con_servidor(cliente_socket, token)
    
    cliente_socket.close()

if __name__ == "__main__":
    main()