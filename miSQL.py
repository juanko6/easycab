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

    #Devuelve tupla de la ubicacion solicitada (x,y)
    def getPosUbicacion(self, IdUbicacion):
        cursor = self.connection.cursor()
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
            existe = False
        finally:            
            cursor.close()

        return posicion

    def checkTaxiId(self, id):
        cursor = self.connection.cursor()
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

    def UpdateClienteTAXI(self, id, cliente):
        cursor = self.connection.cursor()
        try:
            query = """UPDATE TAXI 
                        SET ID_CLIENTE = %s
                    WHERE ID_TAXI = %s"""

            cursor.execute(query, (cliente, id))
            self.connection.commit()
        except mysql.connector.Error as e:
            print(f"Error de MySQL: {e}")        
        finally:
            cursor.close()

    def UpdateEstadoTAXI(self, id, estado, conectado):
        cursor = self.connection.cursor()
        try:
            query = """UPDATE TAXI 
                        SET ESTADO = %s,
                            CONECTADO = %s
                    WHERE ID_TAXI = %s"""

            cursor.execute(query, (estado, conectado, id))
            self.connection.commit()
        except mysql.connector.Error as e:
            print(f"Error de MySQL: {e}")        
        finally:
            cursor.close()            

    def UpdateEstadoCLIENTE(self, id, estado):
        cursor = self.connection.cursor()
        try:
            query = f"""UPDATE CLIENTE 
                        SET ESTADO = '{estado}'
                    WHERE ID_CLIENTE = '{id}'"""

            cursor.execute(query)
            self.connection.commit()
        except mysql.connector.Error as e:
            print(f"Error de MySQL: {e}")        
        finally:
            cursor.close()   

    def UpdateLlegadaCLIENTE(self, id, pos_X, pos_Y, estado):
        cursor = self.connection.cursor()
        try:
            query = f"""UPDATE CLIENTE 
                        SET 
                            ESTADO = '{estado}',
                            POS_X = '{pos_X}',
                            POS_Y = '{pos_Y}'
                    WHERE ID_CLIENTE = '{id}'"""

            cursor.execute(query)
            self.connection.commit()
        except mysql.connector.Error as e:
            print(f"Error de MySQL: {e}")        
        finally:
            cursor.close()   

    #Actualziar o crear si no existe un cliente, y retorna la posición actual del cliente.
    def insertOrUpdateCliente(self, id, des_X, des_Y, estado, pos_X, pos_Y):
        cursor = self.connection.cursor()
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
            
            self.connection.commit()

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

        return posicion

    #finaliza conexión.
    def cerrar(self):
        self.connection.close()