import pyodbc
import json
from datetime import datetime
from decimal import Decimal
from pymongo import MongoClient
from bson import json_util, ObjectId, CodecOptions
import pytz



# Database connection parameters
SQL_SERVER = '192.168.2.72'
SQL_DATABASE = 'vegaallnesrep'
SQL_USERNAME = 'Test10'
SQL_PASSWORD = 'Peru2023'

# MongoDB connection parameters
MONGO_HOST = '192.168.2.49'
MONGO_PORT = 27018
MONGO_DATABASE = 'tradde-nestle'
MONGO_COLLECTION = 'Flex_Mongo_import_promo'

# SQL queries (unchanged)
SQL_QUERY_1 = """

select 
    d.PKID as IDBoni,
    RTRIM(D.Codigo) AS CodigoPromocion,
    RTRIM(D.Descripcion) AS Descripcion,
    DDD.PKID,
    DDD.RutaCaracteristicaEstructural,
    CASE 
        WHEN CHARINDEX('/', RTRIM(DDD.RutaCaracteristicaEstructural)) > 0 
        THEN SUBSTRING(RTRIM(DDD.RutaCaracteristicaEstructural), 
                       LEN(RTRIM(DDD.RutaCaracteristicaEstructural)) - CHARINDEX('/', REVERSE(RTRIM(DDD.RutaCaracteristicaEstructural))) + 2, 
                       LEN(RTRIM(DDD.RutaCaracteristicaEstructural)))
        ELSE DDD.RutaCaracteristicaEstructural
    END AS TABLA,
    rtrim(DDD.ValorDesde) as ValorDesde,
   case when rtrim(Ddd.condicion)='Entre' then rtrim(DDD.ValorHasta)
   when rtrim(Ddd.condicion)='>=' then null else null
     end as ValorHasta
	,
    '' as PorCada
FROM 
    DefinicionDescuento2 D
INNER JOIN 
    DefinicionGrupoReglaDescuento DD ON DD.IDDefinicionDescuento2 = D.PKID
INNER JOIN 
    DefinicionReglaDescuento2 DDD ON DDD.IDDefinicionGrupoReglaDescuento = DD.PKID
WHERE 
    D.PKID = 600078873 and ddd.TieneReglaExclusion=1


"""

SQL_QUERY_2 = """
	WITH CTE AS (
    SELECT  
        d.PKID AS IDBoni,
        DDD.PKID,
        CASE 
            WHEN CHARINDEX('/', RTRIM(DDD.RutaCaracteristicaEstructural)) > 0 
            THEN SUBSTRING(RTRIM(DDD.RutaCaracteristicaEstructural), 
                           LEN(RTRIM(DDD.RutaCaracteristicaEstructural)) - CHARINDEX('/', REVERSE(RTRIM(DDD.RutaCaracteristicaEstructural))) + 2, 
                           LEN(RTRIM(DDD.RutaCaracteristicaEstructural)))
            ELSE rtrim(DDD.RutaCaracteristicaEstructural)
        END AS TABLA,
        CASE
            WHEN rtrim(DDD.ValorDesde) = 'Coleccion' 
            THEN '[' + ISNULL(
                STUFF(( 
                    SELECT ', ' + CONVERT(VARCHAR(100), DDDD.Clave)
                    FROM DefinicionReglaDescuentoValorIncluidoEn DDDD
                    WHERE DDDD.IDDefinicionReglaDescuento2 = DDD.PKID
                    FOR XML PATH('')
                ), 1, 2, ''), '') + ']'
            ELSE rtrim(DDD.ValorDesde)
        END AS ValorDesdeArray,
        ROW_NUMBER() OVER (PARTITION BY 
            CASE 
                WHEN CHARINDEX('/', RTRIM(DDD.RutaCaracteristicaEstructural)) > 0 
                THEN SUBSTRING(RTRIM(DDD.RutaCaracteristicaEstructural), 
                               LEN(RTRIM(DDD.RutaCaracteristicaEstructural)) - CHARINDEX('/', REVERSE(RTRIM(DDD.RutaCaracteristicaEstructural))) + 2, 
                               LEN(RTRIM(DDD.RutaCaracteristicaEstructural)))
                ELSE rtrim(DDD.RutaCaracteristicaEstructural)
            END 
            ORDER BY DDD.PKID
        ) AS Orden
    FROM 
        DefinicionDescuento2 D
    INNER JOIN 
        DefinicionGrupoReglaDescuento DD 
        ON DD.IDDefinicionDescuento2 = D.PKID
    INNER JOIN 
        DefinicionReglaDescuento2 DDD 
        ON DDD.IDDefinicionGrupoReglaDescuento = DD.PKID
    WHERE 
        D.PKID = 600078873 AND DDD.TieneReglaExclusion = 0
)
SELECT *
FROM CTE
ORDER BY TABLA, Orden;
"""


# Add the new SQL query for Obsequios
SQL_QUERY_3 = """
select 
d.PKID as IDBoni, rtrim(d.Codigo) as CodigoRegla, '' as CodigoObsequio,
   null as IDVega, null as Producto,
    null as IDUnidad, null as Cantidad, null as CantidadMaxima,
    null as CantidadMaximaPorCliente, null as DesdeFecha,
   null as HastaFecha, null as TieneStock, null as Stock,
    null as TieneCantidadPorCliente, null as TieneCantidadMax,
    null as Entregado, null as PorEntregar,d.PorcentajeDescuento  as Descuento ,'Descuento' as TipoBono

 from DefinicionDescuento2 d
where d.pkid = 600078873
"""

SQL_QUERY_IDBONI = """
	

	SELECT pkid AS IDBoni,rtrim(Codigo) as CodigoPromocion,rtrim(Descripcion) as Descripcion FROM DefinicionDescuento2
WHERE (Activo=1 and codigo  like '%RUT%EVA%')
"""

# Custom JSON encoder to handle Decimal and ObjectId types
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(CustomJSONEncoder, self).default(obj)
        
        
        
def connect_to_sql_database():
    conn_str = f'DRIVER={{SQL Server}};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD}'
    return pyodbc.connect(conn_str)

def connect_to_mongodb():
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[MONGO_DATABASE]
    return db[MONGO_COLLECTION]

def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

def process_results_1(results):
    if not results:
        return None

    processed_data = {}
    for row in results:
        idboni = row['IDBoni']
        if idboni not in processed_data:
            processed_data[idboni] = {
                "Descripcion": row['Descripcion'],
                "CodigoPromocion": row['CodigoPromocion'],
                "QUERYMONGODB": {"$and": []}
            }
        
        mongodb_query = {}
        valor_desde = row['ValorDesde']
        valor_hasta = row['ValorHasta']
        por_cada = row['PorCada']
        
        field_name = "CantidadProducto" if row['TABLA'] == "CantidadBase" else "PedidoTotal"
        
        if (valor_hasta == 0 or valor_hasta is None or valor_hasta == '') and (por_cada == 0 or por_cada is None or por_cada == ''):
            mongodb_query = {field_name: {"$gte": float(valor_desde)}}
        elif valor_hasta != valor_desde and (por_cada == 0 or por_cada is None or por_cada == ''):
            mongodb_query = {field_name: {"$gte": float(valor_desde), "$lte": float(valor_hasta)}}
        elif (valor_hasta == 0 or valor_hasta is None or valor_hasta == '') and por_cada and por_cada != 0:
            mongodb_query = {field_name: {"$gte": float(valor_desde)}, "PorCada": {"$ne": float(por_cada)}}
        elif valor_desde and valor_hasta and por_cada:
            mongodb_query = {field_name: {"$gte": float(valor_desde), "$lte": float(valor_hasta)}, "PorCada": {"$ne": float(por_cada)}}
        
        if mongodb_query:
            processed_data[idboni]["QUERYMONGODB"]["$and"].append(mongodb_query)
    
    return processed_data
    
def process_results_2(results):
    processed_data = {}
    for row in results:
        idboni = row['IDBoni']
        if idboni not in processed_data:
            processed_data[idboni] = {"QUERYMONGODB": {"$and": []}}
        
        tabla = row['TABLA']
        valor_desde_array = row['ValorDesdeArray']
        orden = row['Orden']
        
        mongodb_query = {}
        
        if tabla in ['Producto', 'Sucursal', 'CategoriaCliente', 'FuerzaVentas', 'Responsable', 'Caracteristica21','Observacion','Marca','ClaseProductoServicio','Caracteristica27','Caracteristica28','Caracteristica29','Caracteristica30','Caracteristica22','Caracteristica23','Caracteristica24','Caracteristica25','Caracteristica26','Caracteristica20','Persona']:
            key_map = {
                'Producto': 'IDVegaProducto',
                'Sucursal': 'IDVegaSucursal',
                'CategoriaCliente': 'IDVegaClienteCategoria',
                'FuerzaVentas': 'IDVegaFuerzaVenta',
                'Responsable': 'IDVegaVendedor',
                'Caracteristica21': 'ClienteCaracteristica21',
                'Observacion': 'ObservacionPedido',
                'Marca': 'IDVegaMarca',
                'ClaseProductoServicio': 'IDVegaLinea',
                'Caracteristica27': 'ClienteCaracteristica27',
                'Caracteristica28': 'ClienteCaracteristica28',
                'Caracteristica29': 'ClienteCaracteristica29',
                'Caracteristica30': 'ClienteCaracteristica30',
                'Caracteristica22': 'ClienteCaracteristica22',
                'Caracteristica23': 'ClienteCaracteristica23',
                'Caracteristica24': 'ClienteCaracteristica24',
                'Caracteristica25': 'ClienteCaracteristica25',
                'Caracteristica26': 'ClienteCaracteristica26',
                'Caracteristica20': 'ClienteCaracteristica20',
                'Persona': 'IDVegaCliente'
                
                
            }
            try:
                values = json.loads(valor_desde_array)
                mongodb_query = {key_map[tabla]: {"$in": values}}
            except json.JSONDecodeError:
                print(f"Warning: Invalid JSON in ValorDesdeArray for {tabla}: {valor_desde_array}")
        elif tabla == 'FechaEmision':
            try:
                # Convertir directamente de dd/mm/yyyy a yyyy-mm-ddT00:00:00.000+00:00
                day, month, year = valor_desde_array.split('/')
                formatted_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}T00:00:00.000+00:00"
                if orden == 1:
                    mongodb_query = {"FechaEmisionPedido": {"$gte": formatted_date}}
                elif orden == 2:
                    mongodb_query = {"FechaEmisionPedido": {"$lte": formatted_date}}
            except ValueError:
                print(f"Error: Unable to parse date: {valor_desde_array}")
        
        if mongodb_query:
            processed_data[idboni]["QUERYMONGODB"]["$and"].append(mongodb_query)
    
    return processed_data

def process_obsequios(results):
    obsequios = []
    for row in results:
        obsequio = {
            "CodigoRegla": row['CodigoRegla'],
            "CodigoObsequio": row['CodigoObsequio'],
            "IDVega": row['IDVega'],
            "Producto": row['Producto'],
            "IDUnidad": row['IDUnidad'],
            "Cantidad": float(row['Cantidad']) if row['Cantidad'] is not None else None,
            "CantidadMaxima": float(row['CantidadMaxima']) if row['CantidadMaxima'] is not None else None,
            "CantidadMaximaPorCliente": float(row['CantidadMaximaPorCliente']) if row['CantidadMaximaPorCliente'] is not None else None,
            "DesdeFecha": row['DesdeFecha'].isoformat() if row['DesdeFecha'] else None,
            "HastaFecha": row['HastaFecha'].isoformat() if row['HastaFecha'] else None,
            "TieneStock": row['TieneStock'],
            "Stock": float(row['Stock']) if row['Stock'] is not None else None,
            "TieneCantidadPorCliente": row['TieneCantidadPorCliente'],
            "TieneCantidadMax": row['TieneCantidadMax'],
            "Entregado": float(row['Entregado']) if row['Entregado'] is not None else None,
            "PorEntregar": float(row['PorEntregar']) if row['PorEntregar'] is not None else None,
            "Descuento": float(row['Descuento']),
            "TipoBono": row['TipoBono']
        }
        obsequios.append(obsequio)
    return obsequios

def merge_results(results1, results2, obsequios, idboni_info):
    merged_data = []
    
    # Si results1 es None (no hay resultados de la primera consulta), inicializar con datos de results2
    if results1 is None:
        results1 = {idboni: {"QUERYMONGODB": {"$and": []}} for idboni in results2.keys()}
    
    for idboni, data in results1.items():
        if idboni in results2:
            data["QUERYMONGODB"]["$and"].extend(results2[idboni]["QUERYMONGODB"]["$and"])
        
        # Añadir Descripcion y CodigoPromocion desde idboni_info
        data["Descripcion"] = idboni_info[idboni]["Descripcion"]
        data["CodigoPromocion"] = idboni_info[idboni]["CodigoPromocion"]
        
        # Filtrar obsequios para este registro específico
        registro_obsequios = [obsequio for obsequio in obsequios if obsequio["CodigoRegla"] == data["CodigoPromocion"]]
        
        # Añadir array de Obsequios como una cadena JSON
        data["Obsequios"] = json.dumps(registro_obsequios, cls=CustomJSONEncoder)
        
        # Convertir QUERYMONGODB a una cadena JSON
        data["QUERYMONGODB"] = json.dumps(data["QUERYMONGODB"], cls=CustomJSONEncoder)
        merged_data.append(data)
    
    # Si no hay resultados de ambas consultas, crear una entrada ficticia
    if not merged_data:
        merged_data.append({
            "Descripcion": "",
            "CodigoPromocion": "",
            "QUERYMONGODB": json.dumps({"$and": []}),
            "Obsequios": "[]"
        })
    
    # Imprimir información de depuración
    for item in merged_data:
        print(f"Merged item:")
        print(f"  CodigoPromocion: {item['CodigoPromocion']}")
        print(f"  Obsequios: {item['Obsequios']}")
    
    return merged_data

def upload_to_mongodb(data, collection):
    try:
        # Define the timezone for Peru/Bogotá
        peru_timezone = pytz.timezone('America/Lima')
        
        # Create a timezone-aware current time
        current_time = datetime.now(peru_timezone)
        
        # Ensure the collection uses the Peru timezone
        collection_with_tz = collection.with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=peru_timezone)
        )
        
        for item in data:
            item['createdAt'] = current_time
            item['updatedAt'] = current_time
        
        result = collection_with_tz.insert_many(data)
        print(f"Successfully inserted {len(result.inserted_ids)} documents into MongoDB")
        
        # Verify the inserted data
        for inserted_id in result.inserted_ids:
            doc = collection_with_tz.find_one({"_id": inserted_id})
            print(f"Inserted document timestamp: {doc['createdAt']}")
    
    except Exception as e:
        print(f"An error occurred while uploading to MongoDB: {e}")

def main():
    sql_connection = None
    try:
        sql_connection = connect_to_sql_database()
        
        mongo_client = MongoClient(MONGO_HOST, MONGO_PORT)
        mongo_db = mongo_client[MONGO_DATABASE]
        mongo_collection = mongo_db[MONGO_COLLECTION]
        
        mongo_collection.create_index([("createdAt", 1)])
        mongo_collection.create_index([("updatedAt", 1)])
        
        # Ejecutar la consulta para obtener la lista de IDBoni
        idboni_list = execute_query(sql_connection, SQL_QUERY_IDBONI)
        print(f"Total de IDBoni encontrados: {len(idboni_list)}")
        
        for idboni_row in idboni_list:
            idboni = idboni_row['IDBoni']
            print(f"\nProcesando IDBoni: {idboni}")
            
            # Obtener información específica para este IDBoni
            current_idboni_info_query = SQL_QUERY_IDBONI.replace("SELECT pkid AS IDBoni", f"SELECT pkid AS IDBoni, Codigo AS CodigoPromocion, Descripcion").replace("WHERE (Activo=1", f"WHERE pkid = {idboni} AND (Activo=1")
            idboni_info_list = execute_query(sql_connection, current_idboni_info_query)
            
            if not idboni_info_list:
                print(f"No se encontró información para IDBoni: {idboni}")
                continue
            
            idboni_info = {idboni: {'CodigoPromocion': idboni_info_list[0]['CodigoPromocion'], 'Descripcion': idboni_info_list[0]['Descripcion']}}
            print(f"Información de IDBoni: {idboni_info}")
            
            current_sql_query_1 = SQL_QUERY_1.replace("D.PKID = 600078873", f"D.PKID = {idboni}")
            current_sql_query_2 = SQL_QUERY_2.replace("D.PKID = 600078873", f"D.PKID = {idboni}")
            current_sql_query_3 = SQL_QUERY_3.replace("d.pkid = 600078873", f"d.pkid = {idboni}")
            
            results1 = execute_query(sql_connection, current_sql_query_1)
            print(f"Resultados de SQL_QUERY_1: {len(results1)}")
            
            results2 = execute_query(sql_connection, current_sql_query_2)
            print(f"Resultados de SQL_QUERY_2: {len(results2)}")
            
            obsequios_results = execute_query(sql_connection, current_sql_query_3)
            print(f"Obsequios encontrados para IDBoni {idboni}: {len(obsequios_results)}")
            print("Detalle de obsequios sin procesar:")
            for obsequio in obsequios_results:
                print(f"  CodigoRegla: {obsequio['CodigoRegla']}, Descuento: {obsequio['Descuento']}, TipoBono: {obsequio['TipoBono']}")
            
            processed_data1 = process_results_1(results1)
            processed_data2 = process_results_2(results2)
            processed_obsequios = process_obsequios(obsequios_results)
            print(f"Obsequios procesados para IDBoni {idboni}: {len(processed_obsequios)}")
            print("Detalle de obsequios procesados:")
            for obsequio in processed_obsequios:
                print(f"  CodigoRegla: {obsequio['CodigoRegla']}, Descuento: {obsequio['Descuento']}, TipoBono: {obsequio['TipoBono']}")
            
            # Process and merge results for this specific IDBoni
            merged_data = merge_results(
                {idboni: processed_data1[idboni]} if processed_data1 else None,
                {idboni: processed_data2[idboni]} if processed_data2 else {},
                processed_obsequios,
                idboni_info
            )
            
            if merged_data:
                upload_to_mongodb(merged_data, mongo_collection)
                print(f"Datos fusionados para IDBoni {idboni}:")
                for item in merged_data:
                    print(f"  Registro: {item['CodigoPromocion']}")
                    print(f"    Descripción: {item['Descripcion']}")
                    print(f"    Obsequios: {item['Obsequios']}")
                    print(f"    QUERYMONGODB: {item['QUERYMONGODB'][:100]}...")  # Mostramos solo los primeros 100 caracteres
            else:
                print(f"No se generaron datos para IDBoni: {idboni}")
            
    except Exception as e:
        print(f"Ocurrió un error: {e}")
        traceback.print_exc()  # Esto imprimirá el traceback completo del error
    finally:
        if sql_connection:
            sql_connection.close()

if __name__ == "__main__":
    main()