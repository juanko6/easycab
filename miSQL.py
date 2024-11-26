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

        self.connection = mysql.connector.connect(**config)
        #self.cursor = self.connection.cursor()

    def consulta(self, query):
        cursor = self.connection.cursor()
        ret = []
        try:
            # Ejecute una consulta
            cursor.execute(query)
            for table in cursor.fetchall():
                ret.append( table)
            cursor.close()
            return ret

        except mysql.connector.Error as e:
            print(f"Error conectándose a MySQL: {e}")
        
        cursor.close()

    def UpdateTAXI(self, id, pos_X, pos_Y, estado, conectado):
        cursor = self.connection.cursor()
        query = f"""UPDATE TAXI 
                    SET POS_X = {pos_X},
                        POS_Y ={pos_Y},
                        ESTADO = '{estado}',
                        CONECTADO = {conectado}
                WHERE ID_TAXI = {id}"""

        cursor.execute(query)
        # Confirmar los cambios
        self.connection.commit()
        cursor.close()

    def insertOrUpdateTAXI(self, id, pos_X, pos_Y, estado):
        cursor = self.connection.cursor()
        query = """INSERT INTO TAXI (ID_TAXI,POS_X,POS_Y,ESTADO)
                    VALUES (%s, %s,%s,%s)
                    ON DUPLICATE KEY UPDATE 
                        ID_TAXI = VALUES(ID_TAXI),
                        POS_X = VALUES(POS_X),
                        POS_Y = VALUES(POS_Y),
                        ESTADO = VALUES(ESTADO)"""

        cursor.execute(query, (id, pos_X, pos_Y, estado))
        # Confirmar los cambios
        self.connection.commit()
        cursor.close()

    def cerrar(self):
        # Cierre la conexión
        self.connection.close()