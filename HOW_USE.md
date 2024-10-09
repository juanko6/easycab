Configuracion de KAFKA
py -m pip install kafka-python
cambiar en el archivo codec.py de 
C:\Users\juank\AppData\Local\Programs\Python\Python312\Lib\site-packages\kafka\codec.py
la ubicación correcta de six
from six.moves import range


para este caso la ip de escucha es la 192.168.1.147

%KAFKA_HOME%\config\zookeeper.properties (se puede configurar el puerto de escucha)
%KAFKA_HOME%\config\server.properties (se debe de configurar IP visible por todos los que vaya a utilizar kafka)

Nota: se tiene que ejecutar en primer lugar zookeeper y en segundo lugar Kafka broker

iniciar zookeeper (1 terminal cmd)
cd %KAFKA_HOME%
%KAFKA_HOME%\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

iniciando broker (2 terminal cmd)
cd %KAFKA_HOME%
%KAFKA_HOME%\bin\windows\kafka-server-start.bat .\config\server.properties

crear topic para solicitudes de clientes: (3 terminal cmd)
cd %KAFKA_HOME%
%KAFKA_HOME%\bin\windows\kafka-topics.bat --create --topic solicitudes-clientes --bootstrap-server 192.168.1.147:9092

Crear el topic para estados de taxis:
cd %KAFKA_HOME%
%KAFKA_HOME%\bin\windows\kafka-topics.bat --create --topic estados-taxis --bootstrap-server 192.168.1.147:9092

Listar topics creados
cd %KAFKA_HOME%
%KAFKA_HOME%\bin\windows\kafka-topics.bat --list --bootstrap-server 192.168.1.147:9092

Ver topic ingresando a kafka
cd %KAFKA_HOME%
%KAFKA_HOME%\bin\windows\kafka-console-consumer.bat --bootstrap-server 192.168.1.147:9092 --topic NOMBREDELTOPIC --from-beginning

iniciar los componentes EC_Central, EC_DE EC_S

py EC_Central.py 5050

py EC_DE.py 192.168.1.147 5050 88 8080 127.0.0.1
ip-central puerto-central idtaxi puerto-sensor ip-sensor

py EC_S.py 127.0.0.1 8080
ip-EC_DE puerto-EC_DE

si se apaga el sensor, el EC_DE queda esperando reconexion

ya en este momento estamos con conexion de sockets y kafka

Enviar una solicitud del cliente se debe de enviar asi:
py EC_Customer.py 192.168.1.147 9092 101 "f18"
ip-broker puerto-broker id-customer direccion-destino

