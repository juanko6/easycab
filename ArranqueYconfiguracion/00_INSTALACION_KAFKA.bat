@echo off

echo Instalar Kafka:
call Z_IntalacionKafka.bat

echo Instalar python:
call Z_InstalacionPython.bat

echo Iniciar Kafka en %KAFKA_HOME%:
rem call iniciarKafka.bat 