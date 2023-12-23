CREATE TABLE IF NOT EXISTS locations_stg (
    location_id INTEGER,
    name VARCHAR(100),
    country VARCHAR(100),
    city VARCHAR(100),
    sensor_type VARCHAR(100),
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    last_updated TIMESTAMP
);
CREATE TABLE IF NOT EXISTS locations (
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

CREATE TABLE IF NOT EXISTS aq_measurements_stg (
    location_id INTEGER,
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
);
CREATE TABLE IF NOT EXISTS aq_measurements (
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
SORTKEY (date);
