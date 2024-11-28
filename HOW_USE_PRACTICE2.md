descargar apache-kafka
variables de entorno
cambiar ip del server


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

en carpeta arranque y configuracion en equipo.config poner la ip compartida del equipo

ejecutar iniciarKafka.bat

en orden iniciar servicios de la app

docker run --name mysqldocker -e MYSQL_ROOT_PASSWORD=1234 -p 3306:3306 -v mysql-data:/var/lib/mysql -d mysql:latest
docker-compose up -d

configurar IP compartida del equipo donde se despliega
py EC_CTC.py

cambiar por IP compartida del equipo donde se despliega
EC_Central.py 192.168.24.1 5050

modificar ip donde va a desplegar el frontend cambiar ip del backend
py frontend.py



configuar cuantos taxis ejecutar
py iniciar_taxis.py



EC_Customer.py a EC_Requests/a_Requests.json


