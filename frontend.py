from flask import Flask, render_template
import requests

app = Flask(__name__, template_folder="templates")

# URL del backend central
BACKEND_URL = "http://192.168.1.140:5000"

@app.route('/')
def dashboard():
    """
    Renderiza el dashboard y obtiene los datos desde el backend central.
    """
    try:
        # Obtener datos de taxis y clientes desde el backend
        response = requests.get(f"{BACKEND_URL}/api/dashboard-data")
        if response.status_code == 200:
            data = response.json()
            return render_template(
                "dashboard.html",
                taxis=data.get("taxis", {}),
                clientes=data.get("clientes", {})
            )
        else:
            return f"Error al obtener datos del backend: {response.status_code}", 500
    except Exception as e:
        return f"Error al conectar con el backend: {e}", 500


if __name__ == "__main__":
    app.run(host="192.168.1.140", port=5500, debug=True)
