"""
    Entregable 02 
    Data Engineering Flex - #56005 
    Coderhouse

    Juan Bohórquez [github.com/JuanBoho]
"""

import requests
import time
import pandas as pd
import sqlalchemy as sa
import logging as log
from datetime import datetime
from configparser import ConfigParser
from retrying import retry

log.basicConfig(level=log.INFO, format="%(levelname)s-%(message)s")

# API
BASE_URL = "https://api.openaq.org/v2"

HEADERS = {"Accept": "application/json"}

LOCATION_IDS = {
    "Argentina": 403227,
    "Chile": 229724,
    "Brasil": 1638491,
    "Bolivia": 1369322,
    "Peru": 287700,
    "Ecuador": 234583,
    "Colombia": 228248,
}

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
    try:
        response = requests.get(url, headers=HEADERS)
        return response if response.status_code == 200 else None

    except Exception as e:
        log.error(f"[get_request] Error: {e}")
        return None


# Lee archivo config
def build_conn_string(config_path, config_section) -> str:
    """
    Construye la cadena de conexión a la base de datos
    a partir de un archivo de configuración.
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
def connect_to_db(conn_string):
    """
    Crea una conexión a la base de datos.
    """
    engine = sa.create_engine(conn_string)
    conn = engine.connect()
    return conn, engine


# Conecta, declara y crea tablas
def create_tables(conn, engine, schema):
    """
    Conexión y creación de tablas en Redshift
    """

    try:
        log.info("Creando tablas en Redshift")
        # Tablas
        create_locations_table = f"""
                DROP TABLE IF EXISTS {schema}.locations;
                CREATE TABLE {schema}.locations (
                    location_id INTEGER distkey,
                    name VARCHAR(100),
                    country VARCHAR(100),
                    city VARCHAR(100),
                    sensor_type VARCHAR(100),
                    longitude DOUBLE PRECISION,
                    latitude DOUBLE PRECISION,
                    last_updated TIMESTAMP
                )
                SORTKEY (location_id);
        """

        create_air_quality_measurements_table = f"""
                DROP TABLE IF EXISTS {schema}.aq_measuraments;
                CREATE TABLE {schema}.aq_measuraments (
                    location_id INTEGER distkey,
                    date DATE,
                    temperature DOUBLE PRECISION,
                    pressure DOUBLE PRECISION,
                    humidity DOUBLE PRECISION,
                    voc DOUBLE PRECISION,
                    um025 DOUBLE PRECISION,
                    um003 DOUBLE PRECISION,
                    um005 DOUBLE PRECISION,
                    um050 DOUBLE PRECISION,
                    um100 DOUBLE PRECISION,
                    pm1 DOUBLE PRECISION,
                    pm10 DOUBLE PRECISION,
                    um010 DOUBLE PRECISION,
                    pm25 DOUBLE PRECISION
                )
                SORTKEY (date)
                ;
        """

        # # Creación de tablas
        with engine.connect() as conn:
            conn.execute(sa.text(create_locations_table))
            conn.execute(sa.text(create_air_quality_measurements_table))
        log.info("Tablas locations y aq_measurements creadas correctamente")
    except Exception as e:
        log.error(f"[create_tables] Error en Redshift: {e}")
        raise e


# [Extracción de datos API]
def get_locations_data(locations) -> list:
    """
    Está función obtiene información sobre las locaciones solicitadas. Realiza una petición individual
    por locación.

    :param locations dict. Un diccionario que contenga pares clave, valor con las locaciones a buscar
    y el id de las mismas. E.g. locations = {'loc1': 1234. 'loc2': 5678}

    Retorna un lista de diccionarios con la inforamción de cada locación.
    """

    endpoint_url = f"{BASE_URL}/locations"

    try:
        log.info(f"Obteniendo información para {len(locations)} locaciones.")
        locations_data = []

        for location, id in locations.items():
            # Petición a la API para cada locación
            url = f"{endpoint_url}/{id}"
            response = get_request(url)

            if response and response.status_code == 200:
                response_data = response.json()

                # Sólo agrega los datos más relevantes de la locación a la lista
                loc_raw_data = response_data.get("results", None)

                if not loc_raw_data or len(loc_raw_data) <= 0:
                    log.info(f"Información vacía para locación: {(location, id)}")
                    continue

                loc_raw_data = loc_raw_data[0]
                loc_data = {
                    "location_id": loc_raw_data.get("id", None),
                    "name": loc_raw_data.get("name", None),
                    "country": loc_raw_data.get("country", None),
                    "city": loc_raw_data.get("city", None),
                    "sensor_type": loc_raw_data.get("sensorType", None),
                    "longitude": loc_raw_data.get("coordinates", None).get(
                        "longitude", None
                    ),
                    "latitude": loc_raw_data.get("coordinates", None).get(
                        "latitude", None
                    ),
                    "last_updated": loc_raw_data.get("lastUpdated", None),
                }

                locations_data.append(loc_data)
            else:
                log.error(
                    f"Error al traer los datos de la locación {(location, id)}. Status: {response.status_code}"
                )

            time.sleep(0.3)  # Algo de espacio entre requests
        return locations_data
    except requests.exceptions.RequestException as e:
        log.error(f"[get_locations_data] Error: {e}")


def get_last_measurements(locations) -> list:
    """
    Está función obtiene información sobre las últimas mediciones para cada locación. Realiza una petición individual
    por locación.

    :param locations dict. Un diccionario que contenga pares clave, valor con las locaciones a buscar
    y el id de las mismas. E.g. locations = {'loc1': 1234. 'loc2': 5678}

    Retorna un lista de diccionarios con la inforamción de cada locación.
    """

    endpoint_url = f"{BASE_URL}/latest/"

    try:
        log.info(f"Obteniendo últimas mediciones para {len(locations)} locaciones.")
        last_measurements = []

        for location, id in locations.items():
            # Petición a la API para cada locación
            url = f"{endpoint_url}/{id}"
            response = get_request(url)

            if response and response.status_code == 200:
                response_data = response.json()

                # Se evaluan los datos devueltos por la API
                loc_raw_data = response_data.get("results", None)

                if not loc_raw_data or len(loc_raw_data) <= 0:
                    log.info(f"Información vacía para locación: {(location, id)}")
                    continue

                measurements = loc_raw_data[0].get("measurements", None)
                if not measurements or len(measurements) <= 0:
                    log.info(f"No hay medidas para locación: {(location, id)}")
                    continue

                # Unifica los datos, incluye el id y los agrega a la lista
                utc_now = datetime.utcnow()
                records_date = datetime(
                    utc_now.year, utc_now.month, utc_now.day, utc_now.hour
                )

                loc_measuraments = {"location_id": id, "date": records_date}
                for item in measurements:
                    parameter = item["parameter"]
                    value = item["value"]
                    loc_measuraments[parameter] = value

                last_measurements.append(loc_measuraments)
            else:
                log.error(f"Error al traer las medidas de la locación {(location, id)}")

            time.sleep(0.3)  # Algo de espacio entre request
        return last_measurements
    except Exception as e:
        log.error(f"[get_last_measurements] Error: {e}")
        raise e


# [Carga de datos a Redshift]
def load_aq_data(data, conn, table="", schema=""):

    """Carga los datos en Redshift.
    :param pandas.DataFrame data. Dataframe con los datos procesados
    :param conn sa.Object. Connexión a Redshift
    :param table str. Nombre de la tabla a donde se cargan los datos
    :param schema str. Schema en el cual se cargan los datos

    """
    try:
        log.info(f"Cargando datos en {table}")
        # Data
        data.to_sql(
            name=table,
            con=conn,
            schema=schema,
            if_exists="replace",
            method="multi",
            index=False,
        )
        log.info(f"Datos cargados en {table}")
    except Exception as e:
        log.error(f"[load_aq_data] Error cargando datos en {table}")
        raise e


# [Ejecución del script]
def run_etl(restart_all=False):
    """Obtiene los datos de la API y los carga en las tablas
    Se espera que corra cada hora cuando se implemente Airflow.

    :param boolean restart_all. Determina si se ejecuta todo el proceso de cero. Por defecto
    se solo se extraen, procesan y cargan los datos. Si se le pasa 'True' crea de cero las tablas
    en Redshift.
    """
    log.info("Inicio de ejecución")
    schema = "juanbohorquez_ar_coderhouse"

    try:
        # Conexión a Redshift
        log.info("Conectando con Redshift...")
        conn_str = build_conn_string("config.ini", "redshift")
        conn, engine = connect_to_db(conn_str)

        if restart_all:
            # Conexión y tablas en Redshift
            create_tables(conn, engine, schema)
            log.info("Tablas creadas en Redshift")

            # Obtiene locaciones
            locations_data = get_locations_data(LOCATION_IDS)
            locations_df = pd.DataFrame(locations_data)

            # Carga info sobre locaciones
            load_aq_data(locations_df, conn, "locations", schema)

        # Obtiene datos diarios
        last_measurements = get_last_measurements(LOCATION_IDS)
        measurements_df = pd.DataFrame(last_measurements)
        load_aq_data(measurements_df, conn, "aq_measuraments", schema)

        log.info("Fin de ejecución")
    except Exception as e:
        log.error(f"Error en ejecución del script: {e}")
        raise e


if __name__ == "__main__":
    run_etl()
