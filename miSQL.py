import mysql.connector
from mysql.connector import pooling
import bcrypt       #instalar TODO:Comprobar que funciona en PC de laboratorio.

class MiSQL():
    def __init__(self):
         # Configuración de la conexión
        config = {
            'host': 'localhost',        # Cambiar si su MySQL está en otro servidor
            'user': 'root',             # Usuario de MySQL
            'password': '1234',         # Contraseña de MySQL
            'database': 'EasyCabDB',      # Nombre de la base de datos
            'port': 3306                # Puerto (3306 es el estándar para MySQL)
        }

        self.pool = pooling.MySQLConnectionPool(
            pool_name="sensor_pool",
            pool_size=50,  # Tamaño del pool de conexiones
            **config
        )

    # Método para hacer consultas genericas pasandole la query por parametro y devolviendo una lista de registros como resultado.
    def consulta(self, query):
        # Obtener una conexión del pool
        connection = self.pool.get_connection()
        cursor = connection.cursor()
        ret = []
        try:
            # Ejecute una consulta
            cursor.execute(query)
            for table in cursor.fetchall():
                ret.append( table)
            return ret
        except mysql.connector.Error as e:
            print(f"Error de MySQL: {e}")        
        finally:
            cursor.close()
            connection.close()


    #Devuelve tupla de la ubicacion solicitada (x,y)
    def getPosUbicacion(self, IdUbicacion):
        # Obtener una conexión del pool
        connection = self.pool.get_connection()
        cursor = connection.cursor()
        query = f"SELECT POS_X, POS_Y FROM UBICACIONES WHERE ID_UBICACION = '{IdUbicacion}'"
        posicion = ()

        try:
            cursor.execute(query)
            resultado = cursor.fetchone()
            if resultado is not None:
                posicion = (resultado[0], resultado[1])
            else:
                posicion = None

        except mysql.connector.Error as e:
            print(f"Error de MySQL: {e}")
        finally:
            cursor.close()
            connection.close()

        return posicion

    def checkTaxiId(self, id):
        # Obtener una conexión del pool
        connection = self.pool.get_connection()
        cursor = connection.cursor()
        query = f"SELECT ID_TAXI, POS_X FROM TAXI WHERE ID_TAXI = {id}"
        
        try:
            cursor.execute(query)
            resultado = cursor.fetchone()
            if resultado is not None:
                existe = True
            else:
                existe = False

        except mysql.connector.Error as e:
            print(f"Error de MySQL: {e}")
            existe = False
        finally:
            cursor.close()
            connection.close()

        return existe

    #Método que actualiza los taxis con posición, estado, conexión.
    def UpdateTAXI(self, id, pos_X, pos_Y, estado, conectado):
        # Obtener una conexión del pool
        connection = self.pool.get_connection()
        cursor = connection.cursor()
        try:
            query = f"""UPDATE TAXI 
                        SET POS_X = {pos_X},
                            POS_Y ={pos_Y},
                            ESTADO = '{estado}',
                            CONECTADO = {conectado}
                    WHERE ID_TAXI = {id}"""

            cursor.execute(query)
            connection.commit()
        except mysql.connector.Error as e:
            print(f"Error de MySQL: {e}")        
        finally:
            cursor.close()
            connection.close()

    def UpdateClienteTAXI(self, id, cliente):
        # Obtener una conexión del pool
        connection = self.pool.get_connection()
        cursor = connection.cursor()
        try:
            query = """UPDATE TAXI 
                        SET ID_CLIENTE = %s
                    WHERE ID_TAXI = %s"""

            cursor.execute(query, (cliente, id))
            connection.commit()
        except mysql.connector.Error as e:
            print(f"Error de MySQL: {e}")        
        finally:
            cursor.close()
            connection.close()

    def UpdateEstadoTAXI(self, id, estado, conectado):
        # Obtener una conexión del pool
        connection = self.pool.get_connection()
        cursor = connection.cursor()
        try:
            query = """UPDATE TAXI 
                        SET ESTADO = %s,
                            CONECTADO = %s
                    WHERE ID_TAXI = %s"""

            cursor.execute(query, (estado, conectado, id))
            connection.commit()
        except mysql.connector.Error as e:
            print(f"Error de MySQL: {e}")        
        finally:
            cursor.close()
            connection.close()          

    def UpdateEstadoCLIENTE(self, id, estado):
        # Obtener una conexión del pool
        connection = self.pool.get_connection()
        cursor = connection.cursor()
        try:
            query = f"""UPDATE CLIENTE 
                        SET ESTADO = '{estado}'
                    WHERE ID_CLIENTE = '{id}'"""

            cursor.execute(query)
            connection.commit()
        except mysql.connector.Error as e:
            print(f"Error de MySQL: {e}")        
        finally:
            cursor.close()
            connection.close()    

    def UpdateLlegadaCLIENTE(self, id, pos_X, pos_Y, estado):
        # Obtener una conexión del pool
        connection = self.pool.get_connection()
        cursor = connection.cursor()
        try:
            query = f"""UPDATE CLIENTE 
                        SET 
                            ESTADO = '{estado}',
                            POS_X = '{pos_X}',
                            POS_Y = '{pos_Y}'
                    WHERE ID_CLIENTE = '{id}'"""

            cursor.execute(query)
            connection.commit()
        except mysql.connector.Error as e:
            print(f"Error de MySQL: {e}")        
        finally:
            cursor.close()
            connection.close() 

    #Actualziar o crear si no existe un cliente, y retorna la posición actual del cliente.
    def insertOrUpdateCliente(self, id, des_X, des_Y, estado, pos_X, pos_Y):
        # Obtener una conexión del pool
        connection = self.pool.get_connection()
        cursor = connection.cursor()
        queryUPD = """INSERT INTO CLIENTE (ID_CLIENTE, DES_X, DES_Y, ESTADO, POS_X, POS_Y)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        DES_X = VALUES(DES_X),
                        DES_Y = VALUES(DES_Y),
                        ESTADO = VALUES(ESTADO),
                        POS_X = VALUES(POS_X),
                        POS_Y = VALUES(POS_Y)
                        """
        
        selectPos = f"SELECT POS_X, POS_Y FROM CLIENTE WHERE ID_CLIENTE = '{id}'"             
        posicion = () 

        try:
            cursor.execute(queryUPD, (id, des_X, des_Y, estado, pos_X, pos_Y))
            
            connection.commit()

            cursor.execute(selectPos)
            resultado = cursor.fetchone()
            if resultado is not None:
                posicion = (resultado[0], resultado[1])
            else:
                posicion = None   

        except mysql.connector.Error as e:
            print(f"Error de MySQL: {e}")        
        finally:
            cursor.close()
            connection.close()

        return posicion


    # Función para registrar un nuevo taxi
    def registrar_usuario(self, id, password):
        # Generar un hash para la contraseña
        password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
        # Obtener una conexión del pool
        connection = self.pool.get_connection()
        cursor = connection.cursor()
        ret = False
        try:
            # Insertar el nuevo usuario con su contraseña hasheada
            query = "INSERT INTO TAXI (ID_TAXI, PASSWORD) VALUES (%s, %s)"
            cursor.execute(query, (id, password_hash))
            connection.commit()  # Confirmar la transacción
            print("Usuario registrado exitosamente.")
            ret = True
        except mysql.connector.Error as err:
            print(f"Error al registrar el usuario: {err}")
        finally:
            cursor.close()
            connection.close()
            
        return ret
    
    # Función para verificar las credenciales de un usuario
    def verificar_loginTAXI(self, id, password):
        # Obtener una conexión del pool
        connection = self.pool.get_connection()
        cursor = connection.cursor()
        ret = False
        try:
            # Buscar el usuario por su id_usuario
            query = "SELECT PASSWORD FROM TAXI WHERE ID_TAXI = %s"
            cursor.execute(query, (id,))
            resultado = cursor.fetchone()

            if resultado:
                # Obtener el hash de la contraseña almacenado
                stored_password_hash = resultado[0]

                # Verificar si la contraseña proporcionada coincide con el hash almacenado
                if bcrypt.checkpw(password.encode('utf-8'), stored_password_hash.encode('utf-8')):
                    print("Autenticación exitosa.")
                    ret = True
                else:
                    print("Contraseña incorrecta.")
            else:
                print("Usuario no encontrado.")

        except mysql.connector.Error as err:
            print(f"Error al verificar el usuario: {err}")
        finally:
            cursor.close()
            connection.close()
        return ret

    #Actualziar o crear si no existe un cliente, y retorna la posición actual del cliente.
    def insertOrUpdate_Token(self, id, token, timeExp):
        # Obtener una conexión del pool
        connection = self.pool.get_connection()
        cursor = connection.cursor()
        queryUPD = """INSERT INTO TOKENS (ID, TOKEN, EXPIRATION)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        TOKEN = VALUES(TOKEN),
                        EXPIRATION = VALUES(EXPIRATION)
                        """
        try:
            cursor.execute(queryUPD, (id, token, timeExp))
            
            connection.commit()

        except mysql.connector.Error as e:
            print(f"Error de MySQL: {e}")        
        finally:
            cursor.close()
            connection.close()

    def verificar_token(self, token):
        # Obtener una conexión del pool
        connection = self.pool.get_connection()
        cursor = connection.cursor()
        query = "SELECT ID FROM TOKENS WHERE TOKEN = %s AND EXPIRATION > NOW()"

        try:
            cursor.execute(query, (token,))
            result = cursor.fetchone()
        except mysql.connector.Error as e:
            print(f"Error de MySQL: {e}")
        finally:            
            cursor.close()
            connection.close()

        if result is not None:
            return result[0]
        else:
            return None

    #finaliza conexión.
    def cerrar(self):
        self.connection.close()