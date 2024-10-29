import subprocess
import configparser
import time

# Leer equipo.config para obtener la IP del servidor central
config = configparser.ConfigParser()
config.read('equipo.config')
ip_central = config.get('Kafka', 'IP', fallback='192.168.56.1')  # Ajustar sección/clave según el archivo

# Parámetros para la configuración
puerto_central = 5050  # Ajusta el puerto central aquí si es necesario
ip_sensor = '127.0.0.1'  # IP de los sensores
id_inicial = 0  # ID inicial de los taxis
num_taxis = 5  # Cambia esto para ajustar la cantidad de taxis a iniciar
puerto_sensor_inicial = 9990  # Puerto de inicio para los sensores

# Iniciar los procesos de taxis y sensores
procesos = []
for i in range(num_taxis):
    taxi_id = id_inicial + i
    puerto_sensor = puerto_sensor_inicial + i
    
    # Comando para iniciar EC_DE.py
    comando_taxi = [
        'py', 'EC_DE.py', ip_central, str(puerto_central), str(taxi_id), ip_sensor, str(puerto_sensor)
    ]
    
    # Comando para iniciar EC_S.py
    comando_sensor = [
        'py', 'EC_S.py', ip_sensor, str(puerto_sensor)
    ]
    
    # Iniciar los procesos de taxi y sensor en una nueva consola
    proceso_taxi = subprocess.Popen(comando_taxi, creationflags=subprocess.CREATE_NEW_CONSOLE)
    proceso_sensor = subprocess.Popen(comando_sensor, creationflags=subprocess.CREATE_NEW_CONSOLE)
    
    # Agregar los procesos a la lista para un seguimiento
    procesos.append(proceso_taxi)
    procesos.append(proceso_sensor)
    
    print(f"Iniciados Taxi {taxi_id} en puerto {puerto_central} y Sensor en puerto {puerto_sensor}")
    
    # Espera opcional entre inicios para evitar conflictos
    time.sleep(1)

# Mantener los procesos abiertos
try:
    # Mantener el script en ejecución
    while True:
        time.sleep(1)  # Mantener el proceso activo
except KeyboardInterrupt:
    print("Interrupción manual. Terminando todos los procesos.")
    # Terminar todos los procesos de taxis y sensores
    for proceso in procesos:
        proceso.terminate()
        proceso.wait()  # Esperar a que el proceso termine correctamente
    print("Todos los procesos han sido terminados.")
