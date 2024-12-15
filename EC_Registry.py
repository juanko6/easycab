from flask import Flask, request, jsonify, render_template
import miSQL
from flask_cors import CORS

# Inicializar la aplicación Flask
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Conectar a la base de datos MySQL
sql = miSQL.MiSQL()

@app.route('/api/registry/alta', methods=['POST'])
def registrar_taxi():
    """
    Endpoint para registrar un nuevo taxi en la base de datos.
    """
    try:
        data = request.get_json()
        taxi_id = data.get('id_taxi')
        password = data.get('password')

        if not taxi_id or not password:
            return jsonify({"error": "Faltan 'id_taxi' o 'password'"}), 400

        # Registrar el taxi en la base de datos
        resultado = sql.registrar_usuario(taxi_id, password)
        
        if resultado:
            return jsonify({"mensaje": f"Taxi {taxi_id} registrado correctamente."}), 201
        else:
            return jsonify({"error": f"No se pudo registrar el taxi {taxi_id}."}), 500
    
    except Exception as e:
        print(f"[ERROR] Error al registrar el taxi: {e}")
        return jsonify({"error": "Error interno del servidor"}), 500


@app.route('/api/registry/baja/<int:taxi_id>', methods=['DELETE'])
def eliminar_taxi(taxi_id):
    """
    Endpoint para dar de baja a un taxi de la base de datos.
    """
    try:
        # Eliminar el taxi de la base de datos
        query = "DELETE FROM TAXI WHERE ID_TAXI = %s"
        cursor = sql.connection.cursor()
        cursor.execute(query, (taxi_id,))
        sql.connection.commit()
        cursor.close()

        if cursor.rowcount > 0:
            return jsonify({"mensaje": f"Taxi {taxi_id} eliminado correctamente."}), 200
        else:
            return jsonify({"error": f"Taxi {taxi_id} no encontrado."}), 404

    except Exception as e:
        print(f"[ERROR] Error al eliminar el taxi: {e}")
        return jsonify({"error": "Error interno del servidor"}), 500


@app.route('/api/registry/listar', methods=['GET'])
def listar_taxis():
    """
    Endpoint para listar todos los taxis registrados en la base de datos.
    """
    try:
        taxis = sql.consulta("SELECT ID_TAXI, POS_X, POS_Y, ESTADO FROM TAXI")
        
        taxis_list = [
            {
                "id_taxi": taxi[0],
                "pos_x": taxi[1],
                "pos_y": taxi[2],
                "estado": taxi[3]
            } for taxi in taxis
        ]

        return jsonify({"taxis": taxis_list}), 200
    except Exception as e:
        print(f"[ERROR] Error al listar los taxis: {e}")
        return jsonify({"error": "Error interno del servidor"}), 500


@app.route('/api/registry/detalle/<int:taxi_id>', methods=['GET'])
def obtener_detalle_taxi(taxi_id):
    """
    Endpoint para obtener los detalles de un taxi en específico.
    """
    try:
        taxi = sql.consulta("SELECT ID_TAXI, POS_X, POS_Y, ESTADO FROM TAXI WHERE ID_TAXI = %s", (taxi_id,))
        
        if taxi:
            taxi_data = {
                "id_taxi": taxi[0][0],
                "pos_x": taxi[0][1],
                "pos_y": taxi[0][2],
                "estado": taxi[0][3]
            }
            return jsonify({"taxi": taxi_data}), 200
        else:
            return jsonify({"error": f"Taxi {taxi_id} no encontrado."}), 404

    except Exception as e:
        print(f"[ERROR] Error al obtener detalles del taxi: {e}")
        return jsonify({"error": "Error interno del servidor"}), 500


@app.route('/dashboard')
def mostrar_dashboard():
    """
    Renderiza el dashboard con los datos actuales de los taxis.
    """
    return render_template('dashboard.html')


if __name__ == "__main__":
    app.run(host="192.168.1.140", port=5002, debug=True)
