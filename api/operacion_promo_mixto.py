import pyodbc
import json
from datetime import datetime
from decimal import Decimal
from bson import json_util, ObjectId, CodecOptions
import pytz

# Database connection parameters
SQL_SERVER = '192.168.2.16'
SQL_DATABASE = 'DIGALIMENTA41' #DIGALIMENTAREP2
SQL_USERNAME = 'Test10'
SQL_PASSWORD = 'Peru2023'

# SQL Queries
SQL_QUERY_IDBONI = """
SELECT pkid AS IDBoni FROM DefinicionBonificacion
WHERE (Activo=? AND (Codigo LIKE ?) AND Historico=0)


"""

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
     end as ValorHasta,
    DDD.PorCada  
FROM 
    DefinicionBonificacion D
INNER JOIN 
    DefinicionGrupoReglaBonificacion DD ON DD.IDDefinicionBonificacion = D.PKID
INNER JOIN 
    DefinicionReglaBonificacion DDD ON DDD.IDDefinicionGrupoReglaBonificacion = DD.PKID
WHERE 
    D.PKID = ? and ddd.TieneReglaExclusion=1
    	and 
	(    CASE 
        WHEN CHARINDEX('/', RTRIM(DDD.RutaCaracteristicaEstructural)) > 0 
        THEN SUBSTRING(RTRIM(DDD.RutaCaracteristicaEstructural), 
                       LEN(RTRIM(DDD.RutaCaracteristicaEstructural)) - CHARINDEX('/', REVERSE(RTRIM(DDD.RutaCaracteristicaEstructural))) + 2, 
                       LEN(RTRIM(DDD.RutaCaracteristicaEstructural)))
        ELSE DDD.RutaCaracteristicaEstructural
    END in ('Total','CantidadBase'))
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
                    FROM DefinicionReglaBonificacionValorIncluidoEn DDDD
                    WHERE DDDD.IDDefinicionReglaBonificacion = DDD.PKID
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
        ) AS Orden , rtrim(ddd.Condicion) as Condicion
    FROM 
        DefinicionBonificacion D
    INNER JOIN 
        DefinicionGrupoReglaBonificacion DD 
        ON DD.IDDefinicionBonificacion = D.PKID
    INNER JOIN 
        DefinicionReglaBonificacion DDD 
        ON DDD.IDDefinicionGrupoReglaBonificacion = DD.PKID
    WHERE 
        D.PKID = ? AND DDD.TieneReglaExclusion = 0

UNION ALL

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
                    FROM DefinicionReglaBonificacionValorIncluidoEn DDDD
                    WHERE DDDD.IDDefinicionReglaBonificacion = DDD.PKID
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
        ) AS Orden , 
		CASE WHEN rtrim(ddd.Condicion)='IncluidoEn' THEN 'IncluidoEn' 
		WHEN rtrim(ddd.Condicion)='NoIncluidoEn' THEN 'NoIncluidoEn' END as Condicion
    FROM 
        DefinicionBonificacion D
    INNER JOIN 
        DefinicionGrupoReglaBonificacion DD 
        ON DD.IDDefinicionBonificacion = D.PKID
    INNER JOIN 
        DefinicionReglaBonificacion DDD 
        ON DDD.IDDefinicionGrupoReglaBonificacion = DD.PKID
    WHERE 
        D.PKID = ? AND DDD.TieneReglaExclusion = 1
		and ( CASE 
            WHEN CHARINDEX('/', RTRIM(DDD.RutaCaracteristicaEstructural)) > 0 
            THEN SUBSTRING(RTRIM(DDD.RutaCaracteristicaEstructural), 
                           LEN(RTRIM(DDD.RutaCaracteristicaEstructural)) - CHARINDEX('/', REVERSE(RTRIM(DDD.RutaCaracteristicaEstructural))) + 2, 
                           LEN(RTRIM(DDD.RutaCaracteristicaEstructural)))
            ELSE rtrim(DDD.RutaCaracteristicaEstructural)
        END not in ('Total','CantidadBase') )
) 
SELECT *
FROM CTE
ORDER BY TABLA, Orden
"""

SQL_QUERY_3 = """
select d.PKID as IDBoni, rtrim(d.Codigo) as CodigoRegla, '' as CodigoObsequio,
    p.IDProducto as IDVega, rtrim(ps.Descripcion) as Producto,
    p.IDUnidad as IDUnidad, p.Cantidad as Cantidad, p.CantidadMaxima as CantidadMaxima,
    p.CantidadMaximaPorCliente as CantidadMaximaPorCliente, p.Desde as DesdeFecha,
    p.Hasta as HastaFecha, p.TieneStock as TieneStock, p.Stock as Stock,
    p.TieneCantidadPorCliente as TieneCantidadPorCliente, '' as TieneCantidadMax,
    p.StockEntregado as Entregado, p.StockPorEntregar as PorEntregar, 'Obsequio' as TipoBono,
    rtrim(u.Descripcion) as Unidad, rtrim(ps.Codigo) as CodigoProducto
from ProductoObsequio2 p
inner join DefinicionBonificacion d on d.PKID = p.IDDefinicionBonificacion
inner join ProductoServicio ps on ps.PKID = p.IDProducto
inner join unidad u on u.PKID = p.IDUnidad
where d.pkid = ?
"""

# Custom JSON encoder
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

def execute_query(connection, query, params):
    cursor = connection.cursor()
    cursor.execute(query, params)
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

def process_results_1(results):
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
        
        # Determine which field to use based on the 'TABLA' column
        field_name = "CantidadProducto" if row['TABLA'] == "CantidadBase" else "PedidoTotal"
        
        if (valor_hasta == 0 or valor_hasta is None or valor_hasta == '') and (por_cada == 0 or por_cada is None or por_cada == ''):
            mongodb_query = {field_name: {"$gte": float(valor_desde)}}
        elif valor_hasta == valor_desde and (por_cada == 0 or por_cada is None or por_cada == ''):
            mongodb_query = {field_name: {"$gte": float(valor_desde), "$lte": float(valor_hasta)}}
        elif (valor_hasta == 0 or valor_hasta is None or valor_hasta == '') and por_cada and por_cada != 0:
            mongodb_query = {field_name: {"$gte": float(valor_desde)}, "PorCada": {"$ne": float(por_cada)}}
        elif valor_desde and valor_hasta and por_cada:
            mongodb_query = {field_name: {"$gte": float(valor_desde), "$lte": float(valor_hasta)}, "PorCada": {"$ne": float(por_cada)}}
        
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
        condicion = row['Condicion']  # New line to get the Condicion value
        
        mongodb_query = {}
        
        if tabla in ['Producto', 'Sucursal', 'CategoriaCliente', 'FuerzaVentas', 'Responsable', 'Caracteristica21','Observacion','Marca','ClaseProductoServicio','Caracteristica27','Caracteristica28','Caracteristica29','Caracteristica30','Caracteristica22','Caracteristica23','Caracteristica24','Caracteristica25','Caracteristica26','Caracteristica20','Persona','Proveedor']:
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
                'Persona': 'IDVegaCliente',
                'Proveedor':'IDVegaProveedor'
            }
            try:
                values = json.loads(valor_desde_array)
                # Modify this part to use $in or $nin based on the Condicion
                operator = "$in" if condicion == "IncluidoEn" else "$nin"
                mongodb_query = {key_map[tabla]: {operator: values}}
            except json.JSONDecodeError:
                print(f"Warning: Invalid JSON in ValorDesdeArray for {tabla}: {valor_desde_array}")
        elif tabla == 'Fecha':
            try:
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
            "Descuento": None,
            "TipoBono": row['TipoBono'],
            "CodigoProducto": row['CodigoProducto'],
            "Unidad": row['Unidad']
        }
        obsequios.append(obsequio)
    return obsequios

def merge_results(results1, results2, obsequios):
    merged_data = []
    for idboni, data in results1.items():
        if idboni in results2:
            data["QUERYMONGODB"]["$and"].extend(results2[idboni]["QUERYMONGODB"]["$and"])
        
        # Add Obsequios array as a JSON string
        data["Obsequios"] = json.dumps([obsequio for obsequio in obsequios if obsequio["CodigoRegla"] == data["CodigoPromocion"]], cls=CustomJSONEncoder)
        
        # Convert QUERYMONGODB to a JSON string
        data["QUERYMONGODB"] = json.dumps(data["QUERYMONGODB"], cls=CustomJSONEncoder)
        merged_data.append(data)
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