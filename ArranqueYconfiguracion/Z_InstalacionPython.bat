@echo off
setlocal enabledelayedexpansion


python -m pip install kafka-python


set "file=C:\Users\alumno\AppData\Roaming\Python\Python312\site-packages\kafka\codec.py"
set "temp_file=temp_file.py"

if exist "%temp_file%" del "%temp_file%"

for /f "usebackq delims=" %%a in ("%file%") do (
    set "line=%%a"
    if "!line!"=="from kafka.vendor.six.moves import range" (
        echo from six.moves import range >> "%temp_file%"
    ) else (
        echo !line! >> "%temp_file%"
    )
)

move /y "%temp_file%" "%file%"

if exist "%temp_file%" del "%temp_file%"

endlocal