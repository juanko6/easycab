@echo off

setlocal enabledelayedexpansion
REM Leer el archivo de configuración línea por línea
for /f "tokens=1,2 delims==" %%a in (equipo.config) do (
    set %%a=%%b
)

xcopy ".\Instalacion\kafka" "C:\kafka\" /E /I /Y
echo "Kafka copiado a C:\"

setx KAFKA_HOME "C:\kafka"
echo La variable de entorno KAFKA_HOME ha sido creada.

rem set NEW_PATH="%KAFKA_HOME%\bin"
rem setx PATH "%PATH%;%NEW_PATH%"
rem echo Se ha añadido %NEW_PATH% a la variable PATH.

set FILE_SERVER="C:\kafka\config\server.properties"

(
    echo listeners=PLAINTEXT://%IP%:%PORT%
    echo advertised.listeners=PLAINTEXT://%IP%:%PORT%
    echo zookeeper.connect=%IP%:%PORTZ%
) >> %FILE_SERVER%

echo Configurado server.propertie kafka

endlocal