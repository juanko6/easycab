@echo off

echo Instalar Kafka:
call Z_IntalacionKafka.bat

echo Instalar python:
timeout /t 4 /nobreak >nul
call Z_InstalacionPython.bat

echo Iniciar Kafka:
timeout /t 4 /nobreak >nul
call iniciarKafka.bat 