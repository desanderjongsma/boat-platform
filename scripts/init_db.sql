CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS telemetry (
    time        TIMESTAMPTZ NOT NULL,
    vessel_id   TEXT NOT NULL,
    topic       TEXT NOT NULL,
    pgn         INTEGER,
    payload     JSONB
);

SELECT create_hypertable('telemetry', 'time', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS vessel_status (
    vessel_id       TEXT PRIMARY KEY,
    online          BOOLEAN DEFAULT FALSE,
    last_seen       TIMESTAMPTZ,
    rpm             FLOAT,
    coolant_temp_c  FLOAT,
    battery_voltage FLOAT,
    battery_soc     FLOAT,
    latitude        FLOAT,
    longitude       FLOAT,
    depth_m         FLOAT,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
