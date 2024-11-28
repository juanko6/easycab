
import configparser
import os

def Entorno():
    config = configparser.ConfigParser()
    config.read('./ArranqueYconfiguracion/equipo.config')
    ip_server = config['Kafka']['IP']
    port_server = config['Kafka']['PORT']
    BOOTSTRAP_SERVER = ip_server+":"+port_server
    print("Configuración [kafka] en " + BOOTSTRAP_SERVER)

    return BOOTSTRAP_SERVER

def LicenciasTaxis():
    config = configparser.ConfigParser()
    config.read('./ArranqueYconfiguracion/equipo.config')  
    max = int(config['LICENCIAS']['MAX'])
    print(f"Máximo de licencias taxis permitidas: {max}")  
    return max

def iniTaxis():
    config = configparser.ConfigParser()
    config.read('./ArranqueYconfiguracion/equipo.config')
    ip_server = config['Kafka']['IP']
    port_server = config['Kafka']['PORT']
    SOLO_SERVER = ip_server
    SOLO_PORT = port_server
    

    return SOLO_SERVER, SOLO_PORT
