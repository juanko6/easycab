import tkinter as tk
import re
import random
import json

# Dimensiones del tablero y el mapa (20x20)
MAPA_FILAS = 20
MAPA_COLUMNAS = 20
TAMANO_CELDA = 30  # Tamaño de cada celda del mapa en píxeles

# Colores para diferentes estados de los taxis
COLORES_TAXI = {
    "disponible": "green",
    "en camino": "blue",
    "incidencia": "orange",
    "detenido": "red"
}

FICHERO_SOLICITUDES = "EC_locations/EC_locations.json"

class Dashboard(tk.Tk):
    def __init__(self):
        super().__init__()
        self.taxis = {}  # Diccionario para guardar las posiciones de los taxis
        self.destinos = {}  # Diccionario para los destinos
        self.ultima_posicion_taxis = {}  # Guardar la última posición de cada taxi
        self.textos_celdas = {}  # Guardar textos en celdas del mapa
        self.title("Dashboard EC_Central")

        # Crear el marco principal
        self.main_frame = tk.Frame(self)
        self.main_frame.pack()

        # Crear el área superior para la tabla de taxis y clientes
        self.tabla_frame = tk.Frame(self.main_frame)
        self.tabla_frame.pack(side=tk.TOP)

        # Etiquetas para la tabla de Taxis
        self.label_taxis = tk.Label(self.tabla_frame, text="Taxis", font=('Arial', 12, 'bold'))
        self.label_taxis.grid(row=0, column=0, columnspan=3)
        self.label_cliente = tk.Label(self.tabla_frame, text="Clientes", font=('Arial', 12, 'bold'))
        self.label_cliente.grid(row=0, column=3, columnspan=3)

        # Títulos de la tabla
        self.titulo_taxis = ['Id', 'Destino', 'Estado']
        self.titulo_clientes = ['Id', 'Destino', 'Estado']

        # Crear títulos de columnas para taxis
        for idx, titulo in enumerate(self.titulo_taxis):
            tk.Label(self.tabla_frame, text=titulo).grid(row=1, column=idx)

        # Crear títulos de columnas para clientes
        for idx, titulo in enumerate(self.titulo_clientes):
            tk.Label(self.tabla_frame, text=titulo).grid(row=1, column=idx+3)

        # Crear el marco para el mapa
        self.mapa_frame = tk.Frame(self.main_frame)
        self.mapa_frame.pack(side=tk.BOTTOM)

        # Crear el canvas para el mapa
        self.canvas = tk.Canvas(self.mapa_frame, width=MAPA_COLUMNAS*TAMANO_CELDA, height=MAPA_FILAS*TAMANO_CELDA)
        self.canvas.pack()

        # Crear el mapa con celdas
        self.mapa = [[None for _ in range(MAPA_COLUMNAS)] for _ in range(MAPA_FILAS)]
        for fila in range(MAPA_FILAS):
            for columna in range(MAPA_COLUMNAS):
                x1 = columna * TAMANO_CELDA
                y1 = fila * TAMANO_CELDA
                x2 = x1 + TAMANO_CELDA
                y2 = y1 + TAMANO_CELDA
                self.mapa[fila][columna] = self.canvas.create_rectangle(x1, y1, x2, y2, fill="white", outline="black")

        # Generar 6 destinos aleatorios
        self.generar_destinos_aleatorios()

        # Guardar la última posición de cada taxi
        self.ultima_posicion_taxis = {}

        # Mostrar los taxis en la tabla
        self.actualizar_tabla_taxis()

        # Iniciar la actualización periódica del mapa
        self.actualizar_mapa_periodicamente()

    def generar_destinos_aleatorios(self):
        # Leer el archivo JSON
        with open(FICHERO_SOLICITUDES, "r") as file:
            data = json.load(file)

        # Obtener la lista de locations
        locations = data["locations"]
        print(f"LocationS: {locations}")
        # Iterar por cada location
        for location in locations:            
            print(f"1-Location: {location}")
            letra = location['Id']
            Pos = location['POS']
            fila, columna = map(int, Pos.split(","))
            if (fila, columna) not in self.destinos:
                self.destinos[(fila, columna)] = letra
                self.canvas.itemconfig(self.mapa[fila][columna], fill="blue")
                self.canvas.create_text(columna * TAMANO_CELDA + TAMANO_CELDA // 2,
                                        fila * TAMANO_CELDA + TAMANO_CELDA // 2,
                                        text=letra, fill="white", font=('Arial', 12, 'bold'))                


    def actualizar_taxis(self):
        for taxi_id, info in self.taxis.items():
            fila, columna = info["posicion"]
            estado = info["estado"]

            # Limpiar la celda anterior del taxi (si existe)
            if taxi_id in self.ultima_posicion_taxis:
                fila_anterior, columna_anterior = self.ultima_posicion_taxis[taxi_id]
                # Restaurar la celda anterior (si era un destino, se restaura con la letra)
                if (fila_anterior, columna_anterior) in self.destinos:
                    self.canvas.itemconfig(self.mapa[fila_anterior][columna_anterior], fill="blue")
                    self.canvas.delete(self.textos_celdas.get((fila_anterior, columna_anterior)))
                    self.textos_celdas[(fila_anterior, columna_anterior)] = self.canvas.create_text(
                        columna_anterior * TAMANO_CELDA + TAMANO_CELDA // 2,
                        fila_anterior * TAMANO_CELDA + TAMANO_CELDA // 2,
                        text=self.destinos[(fila_anterior, columna_anterior)], fill="white", font=('Arial', 12, 'bold')
                    )
                else:
                    self.canvas.itemconfig(self.mapa[fila_anterior][columna_anterior], fill="white")
                    self.canvas.delete(self.textos_celdas.get((fila_anterior, columna_anterior)))

            # Pintar la nueva posición del taxi con su ID y color
            self.canvas.itemconfig(self.mapa[fila][columna], fill=COLORES_TAXI.get(estado.lower(), "white"))
            if (fila, columna) in self.textos_celdas:
                self.canvas.delete(self.textos_celdas[(fila, columna)])  # Borrar el texto anterior en esa celda
            self.textos_celdas[(fila, columna)] = self.canvas.create_text(
                columna * TAMANO_CELDA + TAMANO_CELDA // 2,
                fila * TAMANO_CELDA + TAMANO_CELDA // 2,
                text=str(taxi_id), fill="black", font=('Arial', 12, 'bold')
            )

            # Actualizar la última posición del taxi
            self.ultima_posicion_taxis[taxi_id] = (fila, columna)

    def actualizar_tabla_taxis(self):
        # Mostrar los taxis en la tabla
        for idx, (taxi_id, info) in enumerate(self.taxis.items(), start=2):
            tk.Label(self.tabla_frame, text=str(taxi_id)).grid(row=idx, column=0)
            tk.Label(self.tabla_frame, text="sin destino").grid(row=idx, column=1) 
            tk.Label(self.tabla_frame, text=info["estado"]).grid(row=idx, column=2)

    def leer_fichero_taxis(self):
        try:
            with open("taxis_db.txt", "r") as file:
                lineas = file.readlines()[1:]  # Omitir la cabecera
                for linea in lineas:
                    # Usar regex para capturar taxi_id, posición [x, y], y estado
                    match = re.match(r"(\d+),\[(\d+),(\d+)\],(\w+)", linea.strip())
                    print(f"Partes encontradas: {lineas}")
                    if match:
                        taxi_id = int(match.group(1))
                        x = int(match.group(2))
                        y = int(match.group(3))
                        estado = match.group(4)

                        # Actualizar el diccionario de taxis
                        self.taxis[taxi_id] = {"posicion": [x, y], "estado": estado}
                        print(f"Taxi {taxi_id} - Posición: [{x}, {y}], Estado: {estado}")
                    else:
                        print(f"Error al procesar la línea: {linea.strip()}")
                        print(f"Representación de la línea: {repr(linea)}")
        except FileNotFoundError:
            print("No se encontró el fichero de taxis.")


    def actualizar_mapa(self):
        self.leer_fichero_taxis()  # Leer las posiciones actualizadas
        self.actualizar_taxis()  # Actualizar el mapa con las nuevas posiciones
        self.actualizar_tabla_taxis()  # Actualizar la tabla de taxis

    def actualizar_mapa_periodicamente(self):
        self.actualizar_mapa()
        #print("Mapa actualizado")
        self.after(3000, self.actualizar_mapa_periodicamente)  # Actualizar cada 3 segundos

            
if __name__ == "__main__":
    dashboard = Dashboard()
    dashboard.mainloop()