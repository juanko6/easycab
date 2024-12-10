import socket
import threading
import hashlib
import os
import json
from datetime import datetime, timedelta
import miSQL

# Función para generar un token único
def generar_token():
    return hashlib.sha256(os.urandom(64)).hexdigest()

# Función para validar un token, Devuelve el id_taxi hay que comprobar que es el taxi correcto.
def validar_token(token):
    sql = miSQL.MiSQL()
    return sql.verificar_token(token)

# Función para aunteticar taxi y devolver token.
def autenticar_taxi(cliente_socket, addr):
    print(f"[PETICIÓN AUTENTICACIÓN] {addr} connected.")
    ret = None
    sql = miSQL.MiSQL()
    try:
        # Recibir la solicitud de autenticación (ID y password)
        datos = cliente_socket.recv(1024).decode('utf-8')
        if not datos:
            return None
        
        # Datos recibidos en formato JSON
        datos = json.loads(datos)
        id_taxi = datos.get('id_taxi')
        password = datos.get('password')
        expiration = datetime.now() + timedelta(hours=1) 
        # Intentar autenticar
        if sql.verificar_loginTAXI(id_taxi, password):
            token = generar_token()
            sql.insertOrUpdate_Token(id_taxi, token, expiration)
            #tokens[token] = {'id_taxi': id, 'expiracion': time.time() + 3600}  # Token válido por 1 hora
        if token:
            respuesta = {'mensaje': 'Autenticación exitosa', 'token': token}
            ret = id_taxi
        else:
            respuesta = {'mensaje': 'Autenticación fallida'}

        cliente_socket.send(json.dumps(respuesta).encode('utf-8'))
        
    except Exception as e:
        print(f'Error en comunicación aunteticación: {e}')
    finally:
        cliente_socket.close()
    return ret

#Ejemplo TODO: Modificar por mensajes kafka
def recibir_mensaje(cliente_socket):
    try:
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
        print(f'Error al recibir menasje: {e}')
    finally:
        cliente_socket.close()

# # Pruebas
# if __name__ == "__main__":

#     id = int(input("ID: "))
#     passw = input("password: ")
    
#     sql = miSQL.MiSQL()
#     #sql.registrar_usuario(id, passw)
#     sql.verificar_usuario(id, passw)

