@echo off
setlocal enabledelayedexpansion

echo Parando Server
call 4_stop_ServerKafka.bat
 
timeout /t 5 /nobreak >nul

echo Parando Zookeeper
call 5_stop_Zookeeper.bat
