CREATE SCHEMA IF NOT EXISTS iot;

DROP TABLE IF EXISTS iot.sensors;
DROP TABLE IF EXISTS iot.manufacturers;

CREATE TABLE iot.manufacturers (
    id SMALLINT PRIMARY KEY,
    manufacturer_name VARCHAR(128) NOT NULL UNIQUE,
    country VARCHAR(64) NOT NULL,
    description TEXT
);

CREATE TABLE iot.sensors (
    sensor_id VARCHAR(64) PRIMARY KEY,
    manufacturer_id SMALLINT NOT NULL REFERENCES iot.manufacturers(id)
);

CREATE INDEX idx_sensors_manufacturer_id
    ON iot.sensors(manufacturer_id);