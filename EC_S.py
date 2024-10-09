import sys
import time

# Función para escribir el estado en un fichero
def escribir_estado(estado):
    with open("estado_sensor.txt", "w") as file:
        file.write(estado)

# Función para manejar incidencias del usuario
def manejar_incidencias():
    while True:
        incidencia = input("Introduce una incidencia o 'n' para ninguna (OK):\n[p] Alerta de peatón, [s] Semáforo rojo, [t] Pinchazo, [v] Vehículo cercano, [n] Ninguna\n> ")

        # Asignar el estado basado en la incidencia seleccionada
        if incidencia == 'p':
            estado_actual = "Alerta de peatón"
        elif incidencia == 's':
            estado_actual = "Semáforo rojo"
        elif incidencia == 't':
            estado_actual = "Pinchazo"
        elif incidencia == 'v':
            estado_actual = "Vehículo cercano"
        elif incidencia == 'n':
            estado_actual = "OK"
        else:
            print("Incidencia no válida. Manteniendo estado anterior.")
            estado_actual = "OK"
        
        # Escribir el estado en el fichero
        escribir_estado(estado_actual)
        print(f"Estado del sensor actualizado a: {estado_actual}")
        time.sleep(2)

# Ejecutar el sensor (EC_S)
if __name__ == "__main__":
    # Estado inicial
    escribir_estado("OK")
    print("Estado inicial: OK")

    # Iniciar la gestión de incidencias del usuario
    manejar_incidencias()
