import mysql.connector

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
        #Abrir conexión
        self.connection = mysql.connector.connect(**config)

    # Método para hacer consultas genericas pasandole la query por parametro y devolviendo una lista de registros como resultado.
    def consulta(self, query):
        cursor = self.connection.cursor()
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

    def getPosUbicacion(self, IdUbicacion):
        cursor = self.connection.cursor()
        query = f"SELECT POS_X, POS_Y FROM UBICACIONES WHERE ID_UBICACION = {IdUbicacion}"
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
            existe = False
        finally:            
            cursor.close()
            
        return posicion

    def checkTaxiId(self, id):
        cursor = self.connection.cursor()
        query = f"SELECT ID_TAXI, POS_X FROM TAXI WHERE ID_TAXI = {id}"
        
        try:
            cursor.execute(query)
            resultado = cursor.  fetchone()
            if resultado is not None:
                existe = True
            else:
                existe = False

        except mysql.connector.Error as e:
            print(f"Error de MySQL: {e}")
            existe = False
        finally:            
            cursor.close()

        return existe

    #Método que actualiza los taxis con posición, estado, conexión.
    def UpdateTAXI(self, id, pos_X, pos_Y, estado, conectado):
        cursor = self.connection.cursor()
        try:
            query = f"""UPDATE TAXI 
                        SET POS_X = {pos_X},
                            POS_Y ={pos_Y},
                            ESTADO = '{estado}',
                            CONECTADO = {conectado}
                    WHERE ID_TAXI = {id}"""

            cursor.execute(query)
            self.connection.commit()
        except mysql.connector.Error as e:
            print(f"Error de MySQL: {e}")        
        finally:
            cursor.close()

    #Prueba para actualziar o crear si no existe un taxi.
    def insertOrUpdateTAXI(self, id, pos_X, pos_Y, estado):
        cursor = self.connection.cursor()
        query = """INSERT INTO TAXI (ID_TAXI,POS_X,POS_Y,ESTADO)
                    VALUES (%s, %s,%s,%s)
                    ON DUPLICATE KEY UPDATE 
                        ID_TAXI = VALUES(ID_TAXI),
                        POS_X = VALUES(POS_X),
                        POS_Y = VALUES(POS_Y),
                        ESTADO = VALUES(ESTADO)"""
        try:
            cursor.execute(query, (id, pos_X, pos_Y, estado))
            self.connection.commit()
        except mysql.connector.Error as e:
            print(f"Error de MySQL: {e}")        
        finally:
            cursor.close()

    def cerrar(self):
        # Cerrar la conexión
        self.connection.close()