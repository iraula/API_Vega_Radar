from flask import Flask, request, jsonify
from operacion_promo_pg import (
    connect_to_sql_database as connect_promo,
    execute_query as execute_query_promo,
    upload_to_mongodb as upload_mongodb_promo,
    process_results_1 as process_results_1_promo,
    process_results_2 as process_results_2_promo,
    process_obsequios as process_obsequios_promo,
    merge_results as merge_results_promo,
    SQL_QUERY_IDBONI as SQL_QUERY_IDBONI_PROMO,
    SQL_QUERY_1 as SQL_QUERY_1_PROMO,
    SQL_QUERY_2 as SQL_QUERY_2_PROMO,
    SQL_QUERY_3 as SQL_QUERY_3_PROMO
)
from operacion_descuento_pg import (
    connect_to_sql_database as connect_descuento,
    execute_query as execute_query_descuento,
    upload_to_mongodb as upload_mongodb_descuento,
    process_results_1 as process_results_1_descuento,
    process_results_2 as process_results_2_descuento,
    process_obsequios as process_obsequios_descuento,
    merge_results as merge_results_descuento,
    SQL_QUERY_IDBONI as SQL_QUERY_IDBONI_DESCUENTO,
    SQL_QUERY_1 as SQL_QUERY_1_DESCUENTO,
    SQL_QUERY_2 as SQL_QUERY_2_DESCUENTO,
    SQL_QUERY_3 as SQL_QUERY_3_DESCUENTO
)
from operacion_promo_nestle import (
    connect_to_sql_database as connect_promo_n,
    execute_query as execute_query_promo_n,
    upload_to_mongodb as upload_mongodb_promo_n,
    process_results_1 as process_results_1_promo_n,
    process_results_2 as process_results_2_promo_n,
    process_obsequios as process_obsequios_promo_n,
    merge_results as merge_results_promo_n,
    SQL_QUERY_IDBONI as SQL_QUERY_IDBONI_PROMO_N,
    SQL_QUERY_1 as SQL_QUERY_1_PROMO_N,
    SQL_QUERY_2 as SQL_QUERY_2_PROMO_N,
    SQL_QUERY_3 as SQL_QUERY_3_PROMO_N
)
from operacion_descuento_nestle import (
    connect_to_sql_database as connect_descuento_n,
    execute_query as execute_query_descuento_n,
    upload_to_mongodb as upload_mongodb_descuento_n,
    process_results_1 as process_results_1_descuento_n,
    process_results_2 as process_results_2_descuento_n,
    process_obsequios as process_obsequios_descuento_n,
    merge_results as merge_results_descuento_n,
    SQL_QUERY_IDBONI as SQL_QUERY_IDBONI_DESCUENTO_n,
    SQL_QUERY_1 as SQL_QUERY_1_DESCUENTO_n,
    SQL_QUERY_2 as SQL_QUERY_2_DESCUENTO_n,
    SQL_QUERY_3 as SQL_QUERY_3_DESCUENTO_n
)
from operacion_promo_mixto import (
    connect_to_sql_database as connect_promo_m,
    execute_query as execute_query_promo_m,
    upload_to_mongodb as upload_mongodb_promo_m,
    process_results_1 as process_results_1_promo_m,
    process_results_2 as process_results_2_promo_m,
    process_obsequios as process_obsequios_promo_m,
    merge_results as merge_results_promo_m,
    SQL_QUERY_IDBONI as SQL_QUERY_IDBONI_PROMO_m,
    SQL_QUERY_1 as SQL_QUERY_1_PROMO_m,
    SQL_QUERY_2 as SQL_QUERY_2_PROMO_m,
    SQL_QUERY_3 as SQL_QUERY_3_PROMO_m
)
from operacion_descuento_mixto import (
    connect_to_sql_database as connect_descuento_m,
    execute_query as execute_query_descuento_m,
    upload_to_mongodb as upload_mongodb_descuento_m,
    process_results_1 as process_results_1_descuento_m,
    process_results_2 as process_results_2_descuento_m,
    process_obsequios as process_obsequios_descuento_m,
    merge_results as merge_results_descuento_m,
    SQL_QUERY_IDBONI as SQL_QUERY_IDBONI_DESCUENTO_m,
    SQL_QUERY_1 as SQL_QUERY_1_DESCUENTO_m,
    SQL_QUERY_2 as SQL_QUERY_2_DESCUENTO_m,
    SQL_QUERY_3 as SQL_QUERY_3_DESCUENTO_m
)
from pymongo import MongoClient

app = Flask(__name__)

# MongoDB connection parameters
MONGO_HOST = '192.168.2.49'
MONGO_PORT = 27017
MONGO_DATABASE = 'analytics-b2b'
MONGO_COLLECTION = 'Flex_Mongo_import_promo'

# MongoDB connection parameters
MONGO_HOST_N = '192.168.2.49'
MONGO_PORT_N = 27018
MONGO_DATABASE_N = 'tradde-nestle'
MONGO_COLLECTION_N = 'Flex_Mongo_import_promo'

@app.route('/promos_pg', methods=['POST'])
def get_promos():
    try:
        # Get parameters from request body
        data = request.get_json()
        codigo = data.get('codigo')
        activo = data.get('activo', 1)  # Default value is 1 if not provided

        if not codigo:
            return jsonify({"error": "El parámetro 'codigo' es obligatorio"}), 400

        # Connect to SQL Server database
        sql_connection = connect_promo()

        # Execute query to get IDBoni using codigo and activo parameters
        idboni_list = execute_query_promo(sql_connection, SQL_QUERY_IDBONI_PROMO, (activo, f"%{codigo}%"))
        
        if not idboni_list:
            return jsonify({"message": "No se encontraron promociones para el código proporcionado."}), 404

        # Process SQL query results and obsequios
        merged_data = []
        for idboni_row in idboni_list:
            idboni = idboni_row['IDBoni']
            
            # SQL queries to get data
            results1 = execute_query_promo(sql_connection, SQL_QUERY_1_PROMO, (idboni,))
            results2 = execute_query_promo(sql_connection, SQL_QUERY_2_PROMO, (idboni,))
            obsequios_results = execute_query_promo(sql_connection, SQL_QUERY_3_PROMO, (idboni,))
            
            processed_data1 = process_results_1_promo(results1)
            processed_data2 = process_results_2_promo(results2)
            processed_obsequios = process_obsequios_promo(obsequios_results)
            
            merged_data.extend(merge_results_promo(processed_data1, processed_data2, processed_obsequios))

        # Upload data to MongoDB
        mongo_client = MongoClient(MONGO_HOST, MONGO_PORT)
        mongo_db = mongo_client[MONGO_DATABASE]
        mongo_collection = mongo_db[MONGO_COLLECTION]
        
        upload_mongodb_promo(merged_data, mongo_collection)

        return jsonify({"message": "Se realizó las promos"}), 200
    
    except Exception as e:
        return jsonify({"error": f"Ha ocurrido un error: {str(e)}"}), 500

@app.route('/descuento_pg', methods=['POST'])
def get_descuento_pg():
    sql_connection = None
    mongo_client = None
    try:
        # Get parameters from request body
        data = request.get_json()
        codigo = data.get('codigo')
        activo = data.get('activo', 1)  # Default value is 1 if not provided

        # Validate codigo parameter
        if not codigo:
            return jsonify({"error": "El parámetro 'codigo' es obligatorio"}), 400

        # Connect to SQL Server database
        sql_connection = connect_descuento()

        # Execute initial query to get IDBoni list
        idboni_list = execute_query_descuento(sql_connection, SQL_QUERY_IDBONI_DESCUENTO, (activo, f"%{codigo}%"))

        # Check if no results found
        if not idboni_list:
            return jsonify({"message": "No se encontraron descuentos para el código proporcionado."}), 404

        # Process results for each IDBoni
        merged_data = []
        for idboni_row in idboni_list:
            idboni = idboni_row['IDBoni']
            
            # Obtener información específica para este IDBoni
            idboni_info = {idboni: {
                'CodigoPromocion': idboni_row['CodigoPromocion'],
                'Descripcion': idboni_row['Descripcion']
            }}

            # Ejecutar las consultas para este IDBoni
            results1 = execute_query_descuento(sql_connection, SQL_QUERY_1_DESCUENTO, (idboni,))
            results2 = execute_query_descuento(sql_connection, SQL_QUERY_2_DESCUENTO, (idboni, idboni))
            obsequios_results = execute_query_descuento(sql_connection, SQL_QUERY_3_DESCUENTO, (idboni,))

            # Procesar los resultados
            processed_data1 = process_results_1_descuento(results1)
            processed_data2 = process_results_2_descuento(results2)
            processed_obsequios = process_obsequios_descuento(obsequios_results)

            # Merge results para este IDBoni específico
            batch_merged_data = merge_results_descuento(
                {idboni: processed_data1[idboni]} if processed_data1 else None,
                {idboni: processed_data2[idboni]} if processed_data2 else {},
                processed_obsequios,
                idboni_info
            )

            if batch_merged_data:
                merged_data.extend(batch_merged_data)

        # Upload to MongoDB
        try:
            mongo_client = MongoClient(MONGO_HOST, MONGO_PORT)
            mongo_db = mongo_client[MONGO_DATABASE]
            mongo_collection = mongo_db[MONGO_COLLECTION]
            
            # Crear índices si no existen
            mongo_collection.create_index([("createdAt", 1)], background=True)
            mongo_collection.create_index([("updatedAt", 1)], background=True)
            
            upload_mongodb_descuento(merged_data, mongo_collection)
        except Exception as mongo_error:
            return jsonify({"error": f"Error al guardar en MongoDB: {str(mongo_error)}"}), 500

        return jsonify({
            "message": "Descuentos procesados y cargados exitosamente.",
            "records_processed": len(merged_data)
        }), 200

    except Exception as e:
        return jsonify({"error": f"Ha ocurrido un error: {str(e)}"}), 500
    finally:
        if sql_connection:
            sql_connection.close()
        if mongo_client:
            mongo_client.close()

@app.route('/promos_nestle', methods=['POST'])
def get_promos_n():
    try:
        # Obtener parámetros del cuerpo de la solicitud
        data = request.get_json()
        codigo = data.get('codigo')
        activo = data.get('activo', 1)  # Valor por defecto es 1 si no se proporciona

        if not codigo:
            return jsonify({"error": "El parámetro 'codigo' es obligatorio"}), 400

        # Conectar a la base de datos SQL
        sql_connection = connect_promo_n()

        # Ejecutar consulta para obtener IDBoni usando los parámetros codigo y activo
        idboni_list = execute_query_promo_n(sql_connection, SQL_QUERY_IDBONI_PROMO_N, (activo, f"%{codigo}%"))
        
        if not idboni_list:
            return jsonify({"message": "No se encontraron promociones para el código proporcionado."}), 404

        # Procesar los resultados de la consulta SQL y los obsequios
        merged_data = []
        for idboni_row in idboni_list:
            idboni = idboni_row['IDBoni']
            
            # Consultas SQL para obtener datos
            results1 = execute_query_promo_n(sql_connection, SQL_QUERY_1_PROMO_N, (idboni,))
            results2 = execute_query_promo_n(sql_connection, SQL_QUERY_2_PROMO_N, (idboni,idboni))
            obsequios_results = execute_query_promo_n(sql_connection, SQL_QUERY_3_PROMO_N, (idboni,))
            
            processed_data1 = process_results_1_promo_n(results1)
            processed_data2 = process_results_2_promo_n(results2)
            processed_obsequios = process_obsequios_promo_n(obsequios_results)
            
            merged_data.extend(merge_results_promo_n(processed_data1, processed_data2, processed_obsequios))

        # Subir datos a MongoDB
        mongo_client = MongoClient(MONGO_HOST_N, MONGO_PORT_N)
        mongo_db = mongo_client[MONGO_DATABASE_N]
        mongo_collection = mongo_db[MONGO_COLLECTION_N]
        
        upload_mongodb_promo_n(merged_data, mongo_collection)

        return jsonify({"message": "Se realizó las promos"}), 200
    
    except Exception as e:
        return jsonify({"error": f"Ha ocurrido un error: {str(e)}"}), 500

@app.route('/descuento_nestle', methods=['POST'])
def get_descuento_nestle():
    sql_connection = None
    mongo_client = None
    try:
        # Get parameters from request body
        data = request.get_json()
        codigo = data.get('codigo')
        activo = data.get('activo', 1)  # Default value is 1 if not provided

        # Validate codigo parameter
        if not codigo:
            return jsonify({"error": "El parámetro 'codigo' es obligatorio"}), 400

        # Connect to SQL Server database
        sql_connection = connect_descuento_n()

        # Execute initial query to get IDBoni list
        idboni_list = execute_query_descuento_n(sql_connection, SQL_QUERY_IDBONI_DESCUENTO_n, (activo, f"%{codigo}%"))

        # Check if no results found
        if not idboni_list:
            return jsonify({"message": "No se encontraron descuentos para el código proporcionado."}), 404

        # Process results for each IDBoni
        merged_data = []
        for idboni_row in idboni_list:
            idboni = idboni_row['IDBoni']
            
            # Obtener información específica para este IDBoni
            idboni_info = {idboni: {
                'CodigoPromocion': idboni_row['CodigoPromocion'],
                'Descripcion': idboni_row['Descripcion']
            }}

            # Ejecutar las consultas para este IDBoni
            results1 = execute_query_descuento_n(sql_connection, SQL_QUERY_1_DESCUENTO_n, (idboni,))
            results2 = execute_query_descuento_n(sql_connection, SQL_QUERY_2_DESCUENTO_n, (idboni))
            obsequios_results = execute_query_descuento_n(sql_connection, SQL_QUERY_3_DESCUENTO_n, (idboni,))

            # Procesar los resultados
            processed_data1 = process_results_1_descuento_n(results1)
            processed_data2 = process_results_2_descuento_n(results2)
            processed_obsequios = process_obsequios_descuento_n(obsequios_results)

            # Merge results para este IDBoni específico
            batch_merged_data = merge_results_descuento_n(
                {idboni: processed_data1[idboni]} if processed_data1 else None,
                {idboni: processed_data2[idboni]} if processed_data2 else {},
                processed_obsequios,
                idboni_info
            )

            if batch_merged_data:
                merged_data.extend(batch_merged_data)

        # Upload to MongoDB
        try:
            mongo_client = MongoClient(MONGO_HOST_N, MONGO_PORT_N)
            mongo_db = mongo_client[MONGO_DATABASE_N]
            mongo_collection = mongo_db[MONGO_COLLECTION_N]
            
            # Crear índices si no existen
            mongo_collection.create_index([("createdAt", 1)], background=True)
            mongo_collection.create_index([("updatedAt", 1)], background=True)
            
            upload_mongodb_descuento_n(merged_data, mongo_collection)
        except Exception as mongo_error:
            return jsonify({"error": f"Error al guardar en MongoDB: {str(mongo_error)}"}), 500

        return jsonify({
            "message": "Descuentos procesados y cargados exitosamente.",
            "records_processed": len(merged_data)
        }), 200

    except Exception as e:
        return jsonify({"error": f"Ha ocurrido un error: {str(e)}"}), 500
    finally:
        if sql_connection:
            sql_connection.close()
        if mongo_client:
            mongo_client.close()

@app.route('/promos_mixto', methods=['POST'])
def get_promos_m():
    try:
        # Obtener parámetros del cuerpo de la solicitud
        data = request.get_json()
        codigo = data.get('codigo')
        activo = data.get('activo', 1)  # Valor por defecto es 1 si no se proporciona

        if not codigo:
            return jsonify({"error": "El parámetro 'codigo' es obligatorio"}), 400

        # Conectar a la base de datos SQL
        sql_connection = connect_promo_m()

        # Ejecutar consulta para obtener IDBoni usando los parámetros codigo y activo
        idboni_list = execute_query_promo_m(sql_connection, SQL_QUERY_IDBONI_PROMO_m, (activo, f"%{codigo}%"))
        
        if not idboni_list:
            return jsonify({"message": "No se encontraron promociones para el código proporcionado."}), 404

        # Procesar los resultados de la consulta SQL y los obsequios
        merged_data = []
        for idboni_row in idboni_list:
            idboni = idboni_row['IDBoni']
            
            # Consultas SQL para obtener datos
            results1 = execute_query_promo_m(sql_connection, SQL_QUERY_1_PROMO_m, (idboni,))
            results2 = execute_query_promo_m(sql_connection, SQL_QUERY_2_PROMO_m, (idboni, idboni))
            obsequios_results = execute_query_promo_m(sql_connection, SQL_QUERY_3_PROMO_m, (idboni,))
            
            processed_data1 = process_results_1_promo_m(results1)
            processed_data2 = process_results_2_promo_m(results2)
            processed_obsequios = process_obsequios_promo_m(obsequios_results)
            
            merged_data.extend(merge_results_promo_m(processed_data1, processed_data2, processed_obsequios))

        # Subir datos a MongoDB
        mongo_client = MongoClient(MONGO_HOST, MONGO_PORT)
        mongo_db = mongo_client[MONGO_DATABASE]
        mongo_collection = mongo_db[MONGO_COLLECTION]
        
        upload_mongodb_promo_m(merged_data, mongo_collection)

        return jsonify({"message": "Se realizó las promos"}), 200
    
    except Exception as e:
        return jsonify({"error": f"Ha ocurrido un error: {str(e)}"}), 500

@app.route('/descuento_mixto', methods=['POST'])
def get_descuento_mixto():
    sql_connection = None
    mongo_client = None
    try:
        # Get parameters from request body
        data = request.get_json()
        codigo = data.get('codigo')
        activo = data.get('activo', 1)  # Default value is 1 if not provided

        # Validate codigo parameter
        if not codigo:
            return jsonify({"error": "El parámetro 'codigo' es obligatorio"}), 400

        # Connect to SQL Server database
        sql_connection = connect_descuento_m()

        # Execute initial query to get IDBoni list
        idboni_list = execute_query_descuento_m(sql_connection, SQL_QUERY_IDBONI_DESCUENTO_m, (activo, f"%{codigo}%"))

        # Check if no results found
        if not idboni_list:
            return jsonify({"message": "No se encontraron descuentos para el código proporcionado."}), 404

        # Process results for each IDBoni
        merged_data = []
        for idboni_row in idboni_list:
            idboni = idboni_row['IDBoni']
            
            # Obtener información específica para este IDBoni
            idboni_info = {idboni: {
                'CodigoPromocion': idboni_row['CodigoPromocion'],
                'Descripcion': idboni_row['Descripcion']
            }}

            # Ejecutar las consultas para este IDBoni
            results1 = execute_query_descuento_m(sql_connection, SQL_QUERY_1_DESCUENTO_m, (idboni,))
            results2 = execute_query_descuento_m(sql_connection, SQL_QUERY_2_DESCUENTO_m, (idboni,idboni))
            obsequios_results = execute_query_descuento_m(sql_connection, SQL_QUERY_3_DESCUENTO_m, (idboni,))

            # Procesar los resultados
            processed_data1 = process_results_1_descuento_m(results1)
            processed_data2 = process_results_2_descuento_m(results2)
            processed_obsequios = process_obsequios_descuento_m(obsequios_results)

            # Merge results para este IDBoni específico
            batch_merged_data = merge_results_descuento_m(
                {idboni: processed_data1[idboni]} if processed_data1 else None,
                {idboni: processed_data2[idboni]} if processed_data2 else {},
                processed_obsequios,
                idboni_info
            )

            if batch_merged_data:
                merged_data.extend(batch_merged_data)

        # Upload to MongoDB
        try:
            mongo_client = MongoClient(MONGO_HOST, MONGO_PORT)
            mongo_db = mongo_client[MONGO_DATABASE]
            mongo_collection = mongo_db[MONGO_COLLECTION]
            
            # Crear índices si no existen
            mongo_collection.create_index([("createdAt", 1)], background=True)
            mongo_collection.create_index([("updatedAt", 1)], background=True)
            
            upload_mongodb_descuento_m(merged_data, mongo_collection)
        except Exception as mongo_error:
            return jsonify({"error": f"Error al guardar en MongoDB: {str(mongo_error)}"}), 500

        return jsonify({
            "message": "Descuentos procesados y cargados exitosamente.",
            "records_processed": len(merged_data)
        }), 200

    except Exception as e:
        return jsonify({"error": f"Ha ocurrido un error: {str(e)}"}), 500
    finally:
        if sql_connection:
            sql_connection.close()
        if mongo_client:
            mongo_client.close()


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=3045)