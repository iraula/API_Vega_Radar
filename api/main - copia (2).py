from flask import Flask, request, jsonify
from operacion_promo_pg import (
    connect_to_sql_database,
    execute_query,
    upload_to_mongodb,
    process_results_1,
    process_results_2,
    process_obsequios,
    merge_results,
    SQL_QUERY_IDBONI,
    SQL_QUERY_1,
    SQL_QUERY_2,
    SQL_QUERY_3
)
from pymongo import MongoClient

app = Flask(__name__)

# MongoDB connection parameters
MONGO_HOST = '192.168.2.49'
MONGO_PORT = 27017
MONGO_DATABASE = 'analytics-b2b'
MONGO_COLLECTION = 'Flex_Mongo_import_promo'

@app.route('/promos', methods=['POST'])
def get_promos():
    try:
        # Get parameters from request body
        data = request.get_json()
        codigo = data.get('codigo')
        activo = data.get('activo', 1)  # Default value is 1 if not provided

        if not codigo:
            return jsonify({"error": "El parámetro 'codigo' es obligatorio"}), 400

        # Connect to SQL Server database
        sql_connection = connect_to_sql_database()

        # Execute query to get IDBoni using codigo and activo parameters
        idboni_list = execute_query(sql_connection, SQL_QUERY_IDBONI, (activo, f"%{codigo}%"))
        
        if not idboni_list:
            return jsonify({"message": "No se encontraron promociones para el código proporcionado."}), 404

        # Process SQL query results and obsequios
        merged_data = []
        for idboni_row in idboni_list:
            idboni = idboni_row['IDBoni']
            
            # SQL queries to get data
            results1 = execute_query(sql_connection, SQL_QUERY_1, (idboni,))
            results2 = execute_query(sql_connection, SQL_QUERY_2, (idboni,))
            obsequios_results = execute_query(sql_connection, SQL_QUERY_3, (idboni,))
            
            processed_data1 = process_results_1(results1)
            processed_data2 = process_results_2(results2)
            processed_obsequios = process_obsequios(obsequios_results)
            
            merged_data.extend(merge_results(processed_data1, processed_data2, processed_obsequios))

        # Upload data to MongoDB
        mongo_client = MongoClient(MONGO_HOST, MONGO_PORT)
        mongo_db = mongo_client[MONGO_DATABASE]
        mongo_collection = mongo_db[MONGO_COLLECTION]
        
        upload_to_mongodb(merged_data, mongo_collection)

        return jsonify({"message": "Se realizó las promos"}), 200
    
    except Exception as e:
        return jsonify({"error": f"Ha ocurrido un error: {str(e)}"}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=3045)