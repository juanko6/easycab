@echo off

xcopy "C:\Users\alumno\Downloads\easycab\Instalacion\kafka" "C:\kafka\" /E /I /Y
echo "Kafka copia a C:\"

setx KAFKA_HOME "C:\kafka"
echo La variable de entorno KAFKA_HOME ha sido creada.

set NEW_PATH="%KAFKA_HOME%\bin"
setx PATH "%PATH%;%NEW_PATH%"
echo Se ha añadido %NEW_PATH% a la variable PATH.


set FILE_PATH="C:\kafka\config\server.properties"

(
    echo listeners=PLAINTEXT://172.20.243.41:9092
    echo advertised.listeners=PLAINTEXT://172.20.243.41:9092
    echo zookeeper.connect=172.20.243.41:2181
) >> %FILE_PATH%

pause