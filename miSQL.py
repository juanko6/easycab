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
        self.cursor = self.connection.cursor()

    def consulta(self, query):
        ret = []
        try:
            # Ejecute una consulta
            self.cursor.execute(query)
            for table in self.cursor.fetchall():
                ret.append( table)

            # Cierre la conexión
            self.cursor.close()
            self.connection.close()
            return ret

        except mysql.connector.Error as e:
            print(f"Error conectándose a MySQL: {e}")

    def UpdateTAXI(self, id, pos_X, pos_Y, estado):
        query = f"""UPDATE TAXI 
                    SET POS_X = {pos_X},
                        POS_Y ={pos_Y},
                        ESTADO = {estado}
                WHERE ID_TAXI = {id}"""

        self.cursor.execute(query)
        # Confirmar los cambios
        self.connection.commit()

    def insertOrUpdateTAXI(self, id, pos_X, pos_Y, estado):
        query = """INSERT INTO TAXI (ID_TAXI,POS_X,POS_Y,ESTADO)
                    VALUES (%s, %s,%s,%s)
                    ON DUPLICATE KEY UPDATE 
                        ID_TAXI = VALUES(ID_TAXI),
                        POS_X = VALUES(POS_X),
                        POS_Y = VALUES(POS_Y),
                        ESTADO = VALUES(ESTADO)"""

        self.cursor.execute(query, (id, pos_X, pos_Y, estado))
        # Confirmar los cambios
        self.connection.commit()

    def cerrar(self):
        # Cierre la conexión
        self.cursor.close()
        self.connection.close()