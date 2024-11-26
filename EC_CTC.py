import subprocess
import json
from flask import Flask, request, jsonify

# Configuración de OpenWeather
API_KEY = "0f4c790c8a5bc4c895353ea2c48e"  
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# Inicializar la aplicación Flask
app = Flask(__name__)

# Función para consultar OpenWeather usando curl
def consultar_clima(ciudad):
    try:
        # Comando curl para realizar la solicitud
        url = f"{BASE_URL}?q={ciudad}&appid={API_KEY}&units=metric"
        result = subprocess.run(
            ["curl", "-s", url],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            print(f"Error al ejecutar curl: {result.stderr}")
            return None

        # Parsear el JSON de respuesta
        data = json.loads(result.stdout)
        return data
    except Exception as e:
        print(f"Error al consultar el clima: {e}")
        return None

# Ruta para consultar el tráfico
@app.route('/traffic', methods=['GET'])
def verificar_trafico():
    ciudad = request.args.get('ciudad')
    if not ciudad:
        return jsonify({"error": "Debe proporcionar el nombre de la ciudad"}), 400

    # Consultar el clima
    datos_clima = consultar_clima(ciudad)
    if not datos_clima or "main" not in datos_clima:
        return jsonify({"error": "No se pudo obtener la información del clima"}), 500

    # Obtener la temperatura y determinar el estado del tráfico
    temperatura = datos_clima["main"]["temp"]
    estado_trafico = "OK" if temperatura >= 0 else "KO"

    # Respuesta al cliente
    return jsonify({
        "ciudad": ciudad,
        "temperatura": temperatura,
        "estado_trafico": estado_trafico
    })

# Servidor Flask
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
