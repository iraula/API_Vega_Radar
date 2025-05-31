import pymongo
import pyodbc
import time
from datetime import datetime
from typing import List, Dict, Any
from flask import Flask, jsonify
import threading
import logging
import json

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sync_daemon.log'),
        logging.StreamHandler()
    ]
)

app = Flask(__name__)

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return super().default(obj)

class DatabaseSyncDaemon:
    def __init__(self):
        # Variables para monitoreo
        self.last_sync = None
        self.sync_count = 0
        self.errors = []
        self.is_running = False
        
        # Configuración SQL Server
        self.sql_conn_str = (
            "DRIVER={SQL Server};"
            "SERVER=192.168.2.36;"
            "DATABASE=RadarSoft;"
            "UID=Test10;"
            "PWD=Peru2023;"
            "Trusted_Connection=yes;"
        )
        
        # Configuración MongoDB
        self.mongo_client = pymongo.MongoClient("mongodb://192.168.2.49:27017/")
        self.db = self.mongo_client["analytics-b2b"]
        self.collection = self.db["pedidoprueba2"]
        
        # Eliminar todos los índices existentes excepto el _id
        self.collection.drop_indexes()
        
        # Crear índices compuestos para optimizar búsquedas
        self.collection.create_index([("IDPedido", 1), ("ItemID", 1)], unique=True)

    def convert_types(self, row_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Convierte tipos de datos específicos"""
        try:
            # Convertir IDVegaCliente a entero
            if 'IDPedido' in row_dict and row_dict['IDPedido'] is not None:
                row_dict['IDPedido'] = str(row_dict['IDPedido'])
                
            if 'IDVegaCliente' in row_dict and row_dict['IDVegaCliente'] is not None:
                row_dict['IDVegaCliente'] = int(row_dict['IDVegaCliente'])
            
            if 'IDVegaProducto' in row_dict and row_dict['IDVegaProducto'] is not None:
                row_dict['IDVegaProducto'] = int(row_dict['IDVegaProducto'])
                
            if 'IDVegaVendedor' in row_dict and row_dict['IDVegaVendedor'] is not None:
                row_dict['IDVegaVendedor'] = int(row_dict['IDVegaVendedor'])
            
            if 'IDUnidadProductoPedido' in row_dict and row_dict['IDUnidadProductoPedido'] is not None:
                row_dict['IDUnidadProductoPedido'] = int(row_dict['IDUnidadProductoPedido'])
             
            
            if 'CantidadProducto' in row_dict and row_dict['CantidadProducto'] is not None:
                row_dict['CantidadProducto'] = int(row_dict['CantidadProducto'])
                
            # Convertir PrecioProducto a float
            if 'PrecioProducto' in row_dict and row_dict['PrecioProducto'] is not None:
                row_dict['PrecioProducto'] = float(row_dict['PrecioProducto'])
            
            return row_dict
        except Exception as e:
            error_msg = f"Error al convertir tipos de datos: {str(e)}"
            logging.error(error_msg)
            self.errors.append({
                'timestamp': datetime.now(),
                'error': error_msg
            })
            return row_dict

    def convert_dates(self, row_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Convierte las fechas de SQL Server a objetos datetime de Python"""
        try:
            # Convertir FechaEmisionPedido y FechaEntregaPedido a datetime
            if 'FechaEmisionPedido' in row_dict and row_dict['FechaEmisionPedido']:
                if isinstance(row_dict['FechaEmisionPedido'], str):
                    row_dict['FechaEmisionPedido'] = datetime.strptime(row_dict['FechaEmisionPedido'], '%Y-%m-%d').date()
                row_dict['FechaEmisionPedido'] = datetime.combine(
                    row_dict['FechaEmisionPedido'],
                    datetime.min.time()
                )
            
            if 'FechaEntregaPedido' in row_dict and row_dict['FechaEntregaPedido']:
                if isinstance(row_dict['FechaEntregaPedido'], str):
                    row_dict['FechaEntregaPedido'] = datetime.strptime(row_dict['FechaEntregaPedido'], '%Y-%m-%d').date()
                row_dict['FechaEntregaPedido'] = datetime.combine(
                    row_dict['FechaEntregaPedido'],
                    datetime.min.time()
                )
            
            return row_dict
        except Exception as e:
            error_msg = f"Error al convertir fechas: {str(e)}"
            logging.error(error_msg)
            self.errors.append({
                'timestamp': datetime.now(),
                'error': error_msg
            })
            return row_dict
    
    def check_order_cancelled(self, id_pedido: int) -> bool:
        """Verifica si un pedido está anulado en SQL Server"""
        try:
            with pyodbc.connect(self.sql_conn_str) as conn:
                cursor = conn.cursor()
                query = """
                select    anulado 
                from SVBD100.DIGALIMENTAREP2.DBO.cp 
                where pkid = ?
                """
                cursor.execute(query, (id_pedido,))
                result = cursor.fetchone()
                return result[0] == 1 if result else False
        except Exception as e:
            error_msg = f"Error al verificar anulación: {str(e)}"
            logging.error(error_msg)
            self.errors.append({
                'timestamp': datetime.now(),
                'error': error_msg
            })
            return False

    def get_sql_orders(self) -> List[Dict[Any, Any]]:
        """Obtiene los pedidos de SQL Server del día actual"""
        try:
            with pyodbc.connect(self.sql_conn_str) as conn:
                cursor = conn.cursor()
                query = """
                select c.IDCp AS IDPedido,
                       CAST(C.IDPersona AS INT) AS IDVegaCliente,
                       rtrim(pp.nombre) as ClienteNombreCompleto,
                       rtrim(pp.codigo) as ClienteCodigo,
                       pp.activo as ClienteActivo,
                       d.IDProducto as IDVegaProducto,
                       ps.activo as ProductoActivo,
                       rtrim(ps.codigo) as ProductoCodigo,
                       rtrim(ps.descripcion) as ProductoNombre,
                       c.IDResponsable as IDVegaVendedor,
                       rtrim(p3.codigo) as VendedorCodigo,
                       rtrim(p3.nombre) as VendedorNombreCompleto,
                       d.id as ItemID,
                       cast(c.FechaRegistro as date) as FechaEmisionPedido,
                       cast(c.FechaRegistro as date) as FechaEntregaPedido,
                       c.Observacion as ObservacionPedido,
                       rtrim(u.descripcion) as Unidad,
                       d.IDUnidad as IDUnidadProductoPedido,
                       d.Cantidad as CantidadProducto,
                       CAST(d.Total AS DECIMAL(18,2)) as PrecioProducto
                from tpb_Cabecera c 
                inner join tpb_PedidoDetalle d on c.id=d.PedidoID
                INNER JOIN SVBD100.DIGALIMENTAREP2.DBO.persona pp on pp.pkid=c.idpersona 
                inner join SVBD100.DIGALIMENTAREP2.DBO.productoservicio ps on ps.pkid=d.IDProducto
                inner join SVBD100.DIGALIMENTAREP2.DBO.persona p3 on p3.pkid=c.IDResponsable
                inner join SVBD100.DIGALIMENTAREP2.DBO.unidad u on u.pkid=d.IDUnidad
                where c.Proveedor='YALO' 
                AND c.ESTADO='GESTIONADO'
                AND CAST(c.FechaRegistro AS DATE) = CAST(GETDATE() AS DATE)
                """
                cursor.execute(query)
                columns = [column[0] for column in cursor.description]
                results = []
                for row in cursor.fetchall():
                    row_dict = dict(zip(columns, row))
                    # Convertir fechas y tipos de datos específicos
                    row_dict = self.convert_dates(row_dict)
                    row_dict = self.convert_types(row_dict)
                    results.append(row_dict)
                return results
        except Exception as e:
            error_msg = f"Error al obtener datos de SQL Server: {str(e)}"
            logging.error(error_msg)
            self.errors.append({
                'timestamp': datetime.now(),
                'error': error_msg
            })
            return []

    def item_exists(self, id_pedido: int, item_id: int) -> bool:
        """Verifica si un item específico de un pedido ya existe en MongoDB"""
        try:
            existing_item = self.collection.find_one({
                "IDPedido": id_pedido,
                "ItemID": item_id
            })
            return existing_item is not None
        except Exception as e:
            error_msg = f"Error al verificar existencia del item {item_id} del pedido {id_pedido}: {str(e)}"
            logging.error(error_msg)
            self.errors.append({
                'timestamp': datetime.now(),
                'error': error_msg
            })
            return False

    def delete_order_items(self, id_pedido: int):
        """Elimina todos los items de un pedido que esté anulado en MongoDB"""
        try:
            self.collection.delete_many({"IDPedido": id_pedido})
            logging.info(f"Todos los items del pedido {id_pedido} han sido eliminados de MongoDB")
        except Exception as e:
            error_msg = f"Error al eliminar items del pedido {id_pedido} en MongoDB: {str(e)}"
            logging.error(error_msg)
            self.errors.append({
                'timestamp': datetime.now(),
                'error': error_msg
            })

    def process_order(self, order: Dict[str, Any]):
        """Procesa un pedido individual"""
        try:
            id_pedido = order['IDPedido']
            item_id = order['ItemID']
            
            # Verificar si el pedido está anulado en SQL Server
            if self.check_order_cancelled(id_pedido):
                logging.info(f"El pedido {id_pedido} está anulado. Eliminando todos los items en MongoDB.")
                self.delete_order_items(id_pedido)
                return
            
            # Verificar si el item específico del pedido ya existe
            if self.item_exists(id_pedido, item_id):
                logging.info(f"Item {item_id} del pedido {id_pedido} ya existe en MongoDB. Omitiendo inserción.")
                return
            
            # Agregar timestamps como datetime
            current_time = datetime.now()
            order['createdAt'] = current_time
            order['updatedAt'] = current_time
            
            # Insertar nuevo registro
            self.collection.insert_one(order)
            logging.info(f"Nuevo item {item_id} del pedido {id_pedido} insertado")
            self.sync_count += 1
        except pymongo.errors.DuplicateKeyError:
            logging.info(f"Item {item_id} del pedido {id_pedido} ya existe (detectado por índice único)")
        except Exception as e:
            error_msg = f"Error procesando pedido {order.get('IDPedido', 'unknown')} item {order.get('ItemID', 'unknown')}: {str(e)}"
            logging.error(error_msg)
            self.errors.append({
                'timestamp': datetime.now(),
                'error': error_msg
            })
    
    def run(self):
        """Ejecuta el daemon"""
        logging.info("Iniciando daemon de sincronización")
        self.is_running = True
        while self.is_running:
            try:
                # Obtener pedidos de SQL Server
                orders = self.get_sql_orders()
                
                # Agrupar los items por IDPedido
                grouped_orders = {}

                for order in orders:
                    id_pedido = order["IDPedido"]
                    
                    # Si el pedido no existe en el grupo, inicializa la estructura
                    if id_pedido not in grouped_orders:
                        current_time = datetime.now()
                        grouped_orders[id_pedido] = {
                            "IDPedido": id_pedido,
                            "IDVegaCliente": order["IDVegaCliente"],
                            "IDVegaVendedor": order["IDVegaVendedor"],
                            "ObservacionPedido": order["ObservacionPedido"],
                            "FechaEmisionPedido": order["FechaEmisionPedido"],
                            "Items": [] , # Lista para los items
                            
                            "createdAt": current_time,
                            "updatedAt": current_time
                        }
                    
                    # Agregar el item al pedido correspondiente
                    mongo_item = {
                        "IDVegaProducto": order["IDVegaProducto"],
                        "PrecioProducto": order["PrecioProducto"],
                        "CantidadProducto": order["CantidadProducto"],
                        "IDUnidadProductoPedido": order["IDUnidadProductoPedido"]
                    }
                    grouped_orders[id_pedido]["Items"].append(mongo_item)

                # Procesar los pedidos agrupados
                for id_pedido, mongo_order in grouped_orders.items():
                    # Verificar si el pedido está anulado en SQL Server
                    if self.check_order_cancelled(id_pedido):
                        # Si el pedido está anulado, eliminar los items de ese pedido en MongoDB
                        self.delete_order_items(id_pedido)
                        logging.info(f"El pedido {id_pedido} está anulado. Se han eliminado sus items en MongoDB.")
                        continue  # Continuar con el siguiente pedido
                    
                    # Verificar si el pedido ya existe en MongoDB
                    existing_order = self.collection.find_one({"IDPedido": id_pedido})
                    if not existing_order:
                        # Si el pedido no existe en MongoDB, insertamos todo el pedido con sus items
                        self.collection.insert_one(mongo_order)
                        logging.info(f"Nuevo pedido {id_pedido} insertado en MongoDB con {len(mongo_order['Items'])} items.")
                    else:
                        logging.info(f"El pedido {id_pedido} ya existe en MongoDB, pero no se actualiza.")

                self.last_sync = datetime.now()

                # Esperar 15 segundos
                time.sleep(15)

            except Exception as e:
                error_msg = f"Error en el ciclo principal: {str(e)}"
                logging.error(error_msg)
                self.errors.append({
                    'timestamp': datetime.now(),
                    'error': error_msg
                })
                time.sleep(15)  # Esperar antes de reintentar

    def stop(self):
        """Detiene el daemon"""
        self.is_running = False

# Instancia global del daemon
daemon = DatabaseSyncDaemon()

@app.route('/status')
def get_status():
    """Endpoint para obtener el estado del daemon"""
    return jsonify({
        'running': daemon.is_running,
        'last_sync': daemon.last_sync.isoformat() if daemon.last_sync else None,
        'sync_count': daemon.sync_count,
        'recent_errors': [
            {
                'timestamp': error['timestamp'].isoformat(),
                'error': error['error']
            }
            for error in daemon.errors[-10:]  # Últimos 10 errores
        ]
    })

@app.route('/start')
def start_daemon():
    """Endpoint para iniciar el daemon"""
    if not daemon.is_running:
        thread = threading.Thread(target=daemon.run)
        thread.start()
        return jsonify({'status': 'Daemon iniciado'})
    return jsonify({'status': 'Daemon ya está en ejecución'})

@app.route('/stop')
def stop_daemon():
    """Endpoint para detener el daemon"""
    if daemon.is_running:
        daemon.stop()
        return jsonify({'status': 'Daemon detenido'})
    return jsonify({'status': 'Daemon no está en ejecución'})

if __name__ == "__main__":
    # Iniciar el daemon en un hilo separado
    thread = threading.Thread(target=daemon.run)
    thread.start()
    
    # Iniciar el servidor web
    app.run(host='0.0.0.0', port=3036)
