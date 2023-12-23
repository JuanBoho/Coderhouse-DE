import requests
import smtplib
import sqlalchemy as sa
import logging as log
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
from configparser import ConfigParser
from retrying import retry

log.basicConfig(level=log.INFO, format="%(levelname)s-%(message)s")


# GET request
@retry(
    wait_random_min=250,
    wait_random_max=500,
    stop_max_attempt_number=3,
    retry_on_result=lambda x: x is None,
)
def get_request(url) -> requests.Response:
    """Ejecuta petición GET a la API
    :param url str. Url del endpoint
    """
    headers = {"Accept": "application/json"}
    try:
        response = requests.get(url, headers=headers)
        return response if response.status_code == 200 else None

    except Exception as e:
        log.error(f"[get_request] Error: {e}")
        raise e


# Lee archivo config
def build_conn_string(config_path, config_section) -> str:
    """
    Construye la cadena de conexión a la base de datos
    a partir de un archivo de configuración.
    :param str config_path. Path al archivo de conexión
    :param str config_section. Nombre de la sección en el archivo de configuración.
    """

    # Lee el archivo de configuración
    parser = ConfigParser()
    parser.read(config_path)

    # Lee la sección de configuración
    config = parser[config_section]
    host = config["host"]
    port = config["port"]
    dbname = config["dbname"]
    username = config["username"]
    pwd = config["pwd"]

    # Construye la cadena de conexión
    conn_string = (
        f"postgresql+psycopg2://{username}:{pwd}@{host}:{port}/{dbname}?sslmode=require"
    )
    return conn_string


# Crea conexión
def connect_to_db(conn_string) -> tuple:
    """
    Crea una conexión a la base de datos.
    :param str conn_string. Cadena de texto con los datos de conexión a la bd.
    """
    engine = sa.create_engine(conn_string)
    conn = engine.connect()
    return conn, engine


# [Notificación via email]
def send_email_notification(sender, recipients, report_data, config_path):

    # Lee el archivo de configuración
    parser = ConfigParser()
    parser.read(config_path)

    # Lee la sección de configuración
    config = parser["email"]
    email_secret = config["email_secret"]
    try:
        # Configuración del servidor de email
        email_client = smtplib.SMTP('smtp.gmail.com', 587)
        email_client.starttls()

        # Login en el servidor (cuenta remitente)
        email_client.login(sender, email_secret)

        message = MIMEMultipart()
        message['From'] = sender
        message['To'] = ', '.join(recipients)
        message['Subject'] = 'Reporte diario: Calidad del Aire en Latinoamérica'

        # Contenido del reporte (html)
        body_text = 'Encuentre a continuación el reporte de hoy: '
        message.attach(MIMEText(body_text, 'plain'))
        html_body = f"""
            <html>
                <body>
                    <table border="1">
                        <tr>
                            <th>Country</th>
                            <th>Humidity</th>
                            <th>PM10</th>
                            <th>PM25</th>
                            <th>PM1</th>
                        </tr>
                        {"".join(f"<tr><td>{row['country']}</td><td>{row['Humidity']}</td><td>{row['PM10']}</td><td>{row['PM25']}</td><td>{row['PM1']}</td></tr>" for row in report_data)}
                    </table>
                </body>
            </html>
        """
        message.attach(MIMEText(html_body, 'html'))

        # Envío del email
        email_client.sendmail(
            sender,
            recipients,
            message.as_string()
        )
        log.info(f"Reporte enviado exitosamente. Destinatarios: {', '.join(recipients)}")
    except Exception as e:
        log.error(f"[email_notification] Error: {e}")
        raise e
    finally:
        # Cierra conexión con el servidor de email
        email_client.quit()
