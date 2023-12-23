"""
    Proyecto Final 
    Data Engineering Flex - #56005 
    Coderhouse - 2023

    Juan Bohórquez [github.com/JuanBoho]
"""
import time
import pandas as pd
import logging as log
from requests.exceptions import RequestException
from datetime import datetime
from utils import build_conn_string, connect_to_db, get_request, send_email_notification

log.basicConfig(level=log.INFO, format="%(levelname)s-%(message)s")



class AirQualityETL:
    """
    Obtiene los datos de la API y los carga en las tablas
    Se espera que corra cada hora cuando se implemente Airflow.
    """

    def __init__(self, schema, locations, config_path) -> None:
        self.schema = schema
        self.locations = locations
        self.config_path = config_path
        self.conn_str = build_conn_string(self.config_path, "redshift")


    def get_last_measurements(self, locations) -> list:
        """
        Está función obtiene información sobre las últimas mediciones para cada locación. Realiza una petición individual
        por locación.

        :param locations dict. Un diccionario que contenga pares clave, valor con las locaciones a buscar
        y el id de las mismas. E.g. locations = {'loc1': 1234. 'loc2': 5678}

        Retorna un lista de diccionarios con la inforamción de cada locación.
        """
        # API
        BASE_URL = "https://api.openaq.org/v2"
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

                    loc_measurements = {"location_id": id, "date": records_date}
                    for item in measurements:
                        parameter = item["parameter"]
                        value = item["value"]
                        loc_measurements[parameter] = value

                    last_measurements.append(loc_measurements)
                else:
                    log.warning(f"Error al traer las medidas de la locación {(location, id)}. Status: {response.status_code}")

                time.sleep(0.3)  # Algo de espacio entre request
            return last_measurements
        except Exception as e:
            log.error(f"[get_last_measurements] Error: {e}")
            raise e


    def get_locations_data(self, locations) -> list:
        """
        Está función obtiene información sobre las locaciones solicitadas. Realiza una petición individual
        por locación.

        :param locations dict. Un diccionario que contenga pares clave, valor con las locaciones a buscar
        y el id de las mismas. E.g. locations = {'loc1': 1234. 'loc2': 5678}

        Retorna un lista de diccionarios con la inforamción de cada locación.
        """
        BASE_URL = "https://api.openaq.org/v2"
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
                    log.warning(
                        f"Error al traer los datos de la locación {(location, id)}. Status: {response.status_code}"
                    )

                time.sleep(0.3)  # Algo de espacio entre requests
            return locations_data
        except RequestException as e:
            log.error(f"[get_locations_data] Error: {e}")
            raise e


    def load_aq_data(self, data, conn, table="", schema="", check_field="",check_field_two=""):

        """Carga los datos en Redshift.
        :param pandas.DataFrame data. Dataframe con los datos procesados
        :param conn sa.Object. Connexión a Redshift
        :param table str. Nombre de la tabla a donde se cargan los datos
        :param schema str. Schema en el cual se cargan los datos

        """
        try:
            # Carga de datos en stg
            log.info(f"Cargando datos en la tabla {table}_stg...")
            conn.execute(f"TRUNCATE TABLE {table}_stg")    
            data.to_sql(
                name=table,
                con=conn,
                schema=schema,
                if_exists="append",
                method="multi",
                index=False,
            )
            log.info(f"Datos cargados en {table}_stg")
            log.info(f"Cargando datos en la tabla {table}...")

            extra_check = ";" if check_field_two == "" else f" AND {table}.{check_field_two} = {table}_stg.{check_field_two};"
            log.info(extra_check, check_field_two, check_field)
            sql = f"""BEGIN;
                DELETE FROM {table}
                USING {table}_stg
                WHERE {table}.{check_field} = {table}_stg.{check_field}{extra_check}
            """
            sql += f"""
                INSERT INTO {table}
                SELECT * FROM {table}_stg;
                COMMIT;
            """
            conn.execute(sql)
            log.info("Datos cargados exitosamente")
        except Exception as e:
            log.error(f"[load_aq_data] Error cargando datos en {table}")
            raise e


    def load_locations_data(self):
        """
        Obtiene los datos de la API para las locaciones y los carga en Redshift
        """
        try: 
            locations_data = self.get_locations_data(self.locations)
            locations_df = pd.DataFrame(locations_data)

            # Carga info sobre locaciones
            log.info("Conectando con Redshift...")
            conn, engine = connect_to_db(self.conn_str)

            # Carga info sobre locaciones
            self.load_aq_data(locations_df, conn, "locations", self.schema, "location_id")
        except Exception as e:
            log.error('Error al cargar los datos de locaciones')
            raise e


    def load_aq_measurements_data(self):
        """
        Obtiene los datos de la API para las locaciones y los carga en Redshift
        """
        try: 
            # Obtiene datos diarios
            last_measurements = self.get_last_measurements(self.locations)
            measurements_df = pd.DataFrame(last_measurements)

            # Carga info sobre locaciones
            log.info("Conectando con Redshift...")
            conn, engine = connect_to_db(self.conn_str)

            # Carga info sobre locaciones
            self.load_aq_data(measurements_df, conn, "aq_measurements", self.schema, "location_id", "date")
        except Exception as e:
            log.error('Error al cargar los datos de locaciones')
            raise e


    def build_and_email_report(self, recipients):

        """ Consulta los datos del día en Redshift, crea iu envía un reporte diario via email.
        :param recipients list. Las direcciones de correo electrónico a la que se desea
        enviar el reporte.
        """
        COUNTRIES = {
            "AR":"Argentina",
            "CL":"Chile",
            "BR":"Brasil",
            "NO": "Bolivia",
            "PE":"Perú",
            "EC":"Ecuador",
            "CO":"Colombia"
        }

        try:
            # Extracción de datos para el reporte
            log.info(f"Leyendo últimos datos para el reporte.")
            sql ="""
                SELECT
                    lo.country,
                    m.humidity,
                    m.pm10,
                    m.pm25,
                    m.pm1
                FROM
                    locations lo
                JOIN
                    aq_measurements m ON lo.location_id = m.location_id
                WHERE
                    m.date AT TIME ZONE 'America/Buenos_Aires'::date = CURRENT_DATE;
            """
            conn, engine = connect_to_db(self.conn_str)
            result = engine.execute(sql)
            data = result.fetchall()
            report_data = []
            for row in data:
                country, humidity, pm10, pm25, pm1 = row
                record = {
                    'country': COUNTRIES[country],
                    'Humidity': humidity,
                    'PM10': pm10,
                    'PM25': pm25,
                    'PM1': pm1,
                }
                report_data.append(record)
            log.info("Datos del Reporte extraídos")

            # Enviando reporte via email
            log.info(f"Enviando reporte diario a {len(recipients)} destinatarios.")
            send_email_notification("python.demo.fastapi@gmail.com", recipients, report_data, self.config_path)
            log.info("Fin de ejecución")

        except Exception as e:
            log.error(f"[load_aq_data] Error cargando datos en ")
            raise e