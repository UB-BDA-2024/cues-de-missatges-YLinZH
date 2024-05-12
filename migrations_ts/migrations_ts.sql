-- apply
CREATE TABLE IF NOT EXISTS sensor_data (
    time        TIMESTAMPTZ       NOT NULL,
    sensor_id   INTEGER           NOT NULL,
    velocity    FLOAT             NULL,
    temperature FLOAT             NULL,
    humidity    FLOAT             NULL,
    battery_level FLOAT           NULL,
    PRIMARY KEY (time, sensor_id)
);

-- apply
SELECT create_hypertable('sensor_data', 'time' , if_not_exists => True);
