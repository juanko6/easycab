@echo off
setlocal enabledelayedexpansion

REM Leer el archivo de configuración línea por línea
for /f "tokens=1,2 delims==" %%a in (equipo.config) do (
    set %%a=%%b
)

echo Iniciando Zookeeper
start 1_start_Zookeeper.bat

timeout /t 7 /nobreak >nul

echo Iniciando Server
start 2_start_ServerKafka.bat
 
timeout /t 7 /nobreak >nul

echo Creando topics...

for %%t in (%TOPICS%) do (
    call %KAFKA_HOME%\bin\windows\kafka-topics.bat --create --topic %%t --bootstrap-server %IP%:%PORT%
    IF ERRORLEVEL 1 (
        echo Hubo un error al crear el topic %%t.
        exit /b
    )
)

echo Topics creados:
call %KAFKA_HOME%\bin\windows\kafka-topics.bat --list --bootstrap-server %IP%:%PORT%

pause