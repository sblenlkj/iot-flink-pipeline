INSERT INTO iot.manufacturers (
    id,
    manufacturer_name,
    country,
    description
)
VALUES
    (
        1,
        'Bosch Sensortec',
        'Germany',
        'Manufacturer of MEMS sensors and environmental sensing components'
    ),
    (
        2,
        'Sensirion',
        'Switzerland',
        'Manufacturer of humidity, temperature, gas flow, and environmental sensors'
    ),
    (
        3,
        'Honeywell',
        'United States',
        'Industrial technology company producing sensing and control products'
    ),
    (
        4,
        'Texas Instruments',
        'United States',
        'Semiconductor company producing sensor ICs and embedded measurement components'
    )
ON CONFLICT (id) DO UPDATE
SET
    manufacturer_name = EXCLUDED.manufacturer_name,
    country = EXCLUDED.country,
    description = EXCLUDED.description;

INSERT INTO iot.sensors (
    sensor_id,
    manufacturer_id
)
VALUES
    ('sensor-001', 1),
    ('sensor-002', 1),
    ('sensor-003', 2),
    ('sensor-004', 2),
    ('sensor-005', 3),
    ('sensor-006', 3),
    ('sensor-007', 4),
    ('sensor-008', 4)
ON CONFLICT (sensor_id) DO UPDATE
SET
    manufacturer_id = EXCLUDED.manufacturer_id;