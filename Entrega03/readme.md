Coderhouse  
Data Engineering flex - #56005
**v.1.1** Extración de datos, creación de tablas y carga a Redshift  

# Calidad del Aire en Latinoamérica

El proyecto crea un pipeline que extrae periódicamente datos relacionados con la calidad del aire en las principales ciudades de latinoamérica. Los datos son provistos por [OpenAq](https://openaq.org/) y en principio se centra en la información provista por sensores ubicados en Colombia, Argentina, Chile, Ecuador, Bolivia, Perú y Brasil.

Cada hora se extraerán datos de estos sensores, que serám alamacenados posteriormente en un _DataWarehouse_ en dos tablas _locations_ y _aq_measurements_.

El proyecto tiene como finalidad alimentar el dashboard de una applicación web que permita visualizar los datos dinámicamente.

### Sobre la Fuente
Los datos sobre la calidad del aire provienen de [OpenAq](https://openaq.org/), una plataforma que ofrece información histórica y en tiempo real sobre la calidad del aire en todo el mundo. El pipeline se dirige a los datos de sensores relevantes para ciudades importantes en latinoamérica


### Sobre el Pipeline
El proceso de ETL se ejecutará cada hora para asegurar la disposición de información reciente sobre la calidad del aire. El flujo incluye:

1. Extracción: Se extraen datos de la API de OpenAq, específicamente orientados a los sensores en los países seleccionados.

2. Transformación: Los datos extraídos se tratan para garantizar consistencia, integridad y compatibilidad con el esquema del almacén de datos.

3. Carga: Los datos procesados se cargan en un _Data Warehouse_, utilizando dos tablas principales:

    - **locations**: Almacena información sobre las ubicaciones de los sensores.  

        | location_id | name  | country  | city   | sensor_type | longitude | latitude | last_updated        |
        |-------------|-------|----------|--------|-------------|-----------|----------|---------------------|
        | 123456      | Loc01 | Colombia | Bogotá | ...         | 123.456   | -123.456 | 2023-01-01T20:00:00 |  

    - **aq_measurements**: Contiene mediciones tomadas cada hora sobre las condiciones del aire en cada locación.
       
        | location_id | date                | humidity | temperature | voc   | pm10 | ... | um025 | um100 |
        |-------------|---------------------|----------|-------------|-------|------|-----|-------|-------|
        | 123456      | 2023-01-01T20:00:00 | 50       | 69          | 59.27 | 38.3 | ... | 0.11  | 0.03  |


### Sobre el uso
Para configurar y ejecutar el pipeline localmente, siga estos pasos:

    # Clone el repositorio
    git clone https://github.com/JuanBoho/Coderhouse-DE.git

    # Asegúrese de instalar las dependencias
    pip install -r requirements.txt

    # Ejecute el pipeline
    python main.py


Para configurar y ejecutar el ETL desde Airflow, siga estos pasos:

    # Clone el repositorio
    git clone https://github.com/JuanBoho/Coderhouse-DE.git

    # Asegúrese de instalar las dependencias
    docker compose up -d

_Nota: Lo anterior asume que se dispone de un archivo **config.ini** con los datos y credenciales de conexión para Redshift o su configuración en Airflow._