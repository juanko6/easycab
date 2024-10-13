
import configparser

def Entorno():
    config = configparser.ConfigParser()
    config.read('./Arranque y configuracion/equipo.config')
    ip_server = config['Kafka']['IP']
    port_server = config['Kafka']['PORT']
    BOOTSTRAP_SERVER = ip_server+":"+port_server
    print("Configuración [kafka] en " + BOOTSTRAP_SERVER)

    return BOOTSTRAP_SERVER