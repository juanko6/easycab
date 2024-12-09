import socket
import threading
import hashlib
import os
import json
import time

# Diccionario para almacenar los usuarios (en un caso real, esto sería una base de datos)
usuarios = {
    'usuario1': {'password': 'password123'},  # Aquí deberías usar contraseñas hashadas en una aplicación real
}

# Diccionario para almacenar los tokens activos
tokens = {}

# Función para generar un token único
def generar_token():
    return hashlib.sha256(os.urandom(64)).hexdigest()

# Función para autenticar al usuario
def autenticar_cliente(id_usuario, password):
    if id_usuario in usuarios and usuarios[id_usuario]['password'] == password:
        token = generar_token()
        tokens[token] = {'id_usuario': id_usuario, 'expiracion': time.time() + 3600}  # Token válido por 1 hora
        return token
    return None

# Función para validar un token
def validar_token(token):
    if token in tokens:
        if tokens[token]['expiracion'] > time.time():
            return tokens[token]['id_usuario']
    return None

# Función para manejar la comunicación con el cliente
def manejar_cliente(cliente_socket):
    try:
        # Recibir la solicitud de autenticación (ID y password)
        datos = cliente_socket.recv(1024).decode('utf-8')
        if not datos:
            return
        
        # Datos recibidos en formato JSON
        datos = json.loads(datos)
        id_usuario = datos.get('id_usuario')
        password = datos.get('password')

        # Intentar autenticar
        token = autenticar_cliente(id_usuario, password)
        if token:
            respuesta = {'mensaje': 'Autenticación exitosa', 'token': token}
        else:
            respuesta = {'mensaje': 'Autenticación fallida'}

        cliente_socket.send(json.dumps(respuesta).encode('utf-8'))

        # Esperar el siguiente mensaje con el token
        datos = cliente_socket.recv(1024).decode('utf-8')
        if not datos:
            return
        
        # Verificar el token
        datos = json.loads(datos)
        token = datos.get('token')
        id_usuario_validado = validar_token(token)

        if id_usuario_validado:
            cliente_socket.send(json.dumps({'mensaje': f'Bienvenido, {id_usuario_validado}!'}).encode('utf-8'))
        else:
            cliente_socket.send(json.dumps({'mensaje': 'Token inválido o expirado'}).encode('utf-8'))

    except Exception as e:
        print(f'Error al manejar cliente: {e}')
    finally:
        cliente_socket.close()

#####
##TODO:Sustituir main por llamadas reales desde EC_Central.py
# Configuración del servidor
def iniciar_servidor(host='127.0.0.1', puerto=12345):
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.bind((host, puerto))
    servidor.listen(5)
    print(f'Servidor escuchando en {host}:{puerto}')
    
    while True:
        cliente_socket, cliente_direccion = servidor.accept()
        print(f'Conexión recibida de {cliente_direccion}')
        threading.Thread(target=manejar_cliente, args=(cliente_socket,)).start()

# Iniciar el servidor
if __name__ == "__main__":
    iniciar_servidor()
