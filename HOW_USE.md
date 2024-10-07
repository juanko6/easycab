Configuracion de KAFKA
py -m pip install kafka-python
para este caso la ip de escucha es la 192.168.1.147

%KAFKA_HOME%\config\zookeeper.properties (se puede configurar el puerto de escucha)
KAFKA_HOME%\config\server.properties (se debe de configurar IP visible por todos los que vaya a utilizar kafka)

Nota: se tiene que ejecutar en primer lugar zookeeper y en segundo lugar Kafka broker

iniciar zookeeper 
cd %KAFKA_HOME%
%KAFKA_HOME%\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

iniciando broker
cd %KAFKA_HOME%
%KAFKA_HOME%\bin\windows\kafka-server-start.bat .\config\server.properties

crear topic para solicitudes de clientes:
cd %KAFKA_HOME%
%KAFKA_HOME%\bin\windows\kafka-topics.bat --create --topic solicitudes-clientes --bootstrap-server 192.168.1.147:9092

Crear el topic para estados de taxis:
cd %KAFKA_HOME%
%KAFKA_HOME%\bin\windows\kafka-topics.bat --create --topic estados-taxis --bootstrap-server 192.168.1.147:9092



iniciar los componentes EC_Central, EC_DE EC_S

py EC_Central.py 5050

py EC_DE.py 192.168.1.147 5050 88 8080 127.0.0.1
ip-central puerto-central idtaxi puerto-sensor ip-sensor

python EC_S.py 127.0.0.1 8080
ip-EC_DE puerto-EC_DE

si se apaga el sensor, el EC_DE queda esperando reconexion

ya en este momento estamos con conexion de sockets y kafka



