import pyodbc
import schedule
import time
from datetime import datetime
import pytz
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from flask import Flask, jsonify
import threading

app = Flask(__name__)

# Configuración de la conexión a SQL Server
conn_str = (
    "DRIVER={SQL Server};"
    "SERVER=192.168.2.36;"
    "DATABASE=RadarSoft;"
    "UID=Test10;"
    "PWD=Peru2023;"
)

# Consulta SQL
query = """
select id,idcp,Estado_Promo,estado,rtrim(p.codigo) as CodigoPersona,c.FechaRegistro,
case when c.Estado='GESTIONADO' then 'SE REGISTRÓ BIEN'
ELSE c.Estado END AS 'ESTADO'
,cast(c.Total as decimal(18,4)) as TotalVenta,cp.numcp,case when cp.anulado=1 then 'Anulado en el Flex'
else 'Sigue digitado' end as 'EstadoFlex' from tpb_Cabecera
c inner join  SVBD7524.digalimenta41.dbo.persona p on c.IDPersona=p.pkid
inner join  SVBD7524.digalimenta41.dbo.cp cp on cp.pkid=c.IDCp
where  CONVERT(varchar, FechaRegistro, 23)  =CONVERT(varchar, GETDATE(), 23) 
and proveedor = 'YALO'
"""

# Configuración del correo electrónico
EMAIL_ADDRESS = "iraula.j@grupovega.com.pe"
EMAIL_PASSWORD = "yEc97D12w%Pp"
RECIPIENT = "josue.9.13.11@gmail.com,omar.ruiz@radar365.pe"

# Variable global para almacenar la última fecha de ejecución
ultima_ejecucion = None

def crear_tabla_html(columns, rows):
    html = """
    <html>
    <head>
    <style>
        table {
            border-collapse: collapse;
            width: 100%;
        }
        th, td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }
        th {
            background-color: #f2f2f2;
        }
        tr:nth-child(even) {
            background-color: #f9f9f9;
        }
    </style>
    </head>
    <body>
    <table>
        <tr>
    """
    
    for column in columns:
        html += f"<th>{column}</th>"
    
    html += "</tr>"
    
    for row in rows:
        html += "<tr>"
        for value in row:
            html += f"<td>{value}</td>"
        html += "</tr>"
    
    html += """
    </table>
    </body>
    </html>
    """
    return html

def enviar_correo(asunto, cuerpo_html):
    msg = MIMEMultipart()
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = RECIPIENT
    msg['Subject'] = asunto
    
    msg.attach(MIMEText(cuerpo_html, 'html'))
    
    with smtplib.SMTP('smtp.office365.com', 587) as server:
        server.starttls()
        server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        server.send_message(msg)

def verificar_registros():
    global ultima_ejecucion
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        
        lima_tz = pytz.timezone('America/Lima')
        hora_actual = datetime.now(lima_tz)
        
        if ultima_ejecucion is None:
            registros_nuevos = rows
        else:
            registros_nuevos = [
                row for row in rows 
                if lima_tz.localize(row.FechaRegistro.replace(tzinfo=None)) > ultima_ejecucion
            ]
        
        if registros_nuevos:
            asunto = f"Nuevos registros encontrados: {len(registros_nuevos)}"
            cuerpo_html = crear_tabla_html(columns, registros_nuevos)
            
            enviar_correo(asunto, cuerpo_html)
            print(f"Correo enviado: {len(registros_nuevos)} nuevos registros.")
        else:
            print("No se encontraron nuevos registros.")
        
        ultima_ejecucion = hora_actual
        conn.close()
    except Exception as e:
        print(f"Error: {str(e)}")

def run_schedule():
    while True:
        schedule.run_pending()
        time.sleep(1)

@app.route('/status')
def status():
    return jsonify({"status": "running"})

if __name__ == '__main__':
    # Programar la tarea para que se ejecute cada minuto
    schedule.every(1).minutes.do(verificar_registros)
    
    # Iniciar el planificador en un hilo separado
    scheduler_thread = threading.Thread(target=run_schedule)
    scheduler_thread.start()
    
    # Iniciar el servidor Flask
    app.run(host='0.0.0.0', port=3031)