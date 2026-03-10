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

CREATE TABLE IF NOT EXISTS alert_rules (
    id          SERIAL PRIMARY KEY,
    vessel_id   TEXT NOT NULL,
    name        TEXT NOT NULL,
    metric      TEXT NOT NULL,
    operator    TEXT NOT NULL,
    threshold   FLOAT NOT NULL,
    severity    TEXT NOT NULL DEFAULT 'warning',
    enabled     BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS alerts (
    id              SERIAL PRIMARY KEY,
    vessel_id       TEXT NOT NULL,
    rule_id         INTEGER REFERENCES alert_rules(id) ON DELETE CASCADE,
    rule_name       TEXT NOT NULL,
    metric          TEXT NOT NULL,
    value           FLOAT NOT NULL,
    threshold       FLOAT NOT NULL,
    operator        TEXT NOT NULL,
    severity        TEXT NOT NULL DEFAULT 'warning',
    triggered_at    TIMESTAMPTZ DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ,
    acknowledged    BOOLEAN DEFAULT FALSE,
    acknowledged_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS vessel_profiles (
    vessel_id               TEXT PRIMARY KEY,
    -- Identity
    name                    TEXT,
    vessel_type             TEXT,
    flag                    TEXT,
    call_sign               TEXT,
    mmsi                    TEXT,
    imo                     TEXT,
    registration_number     TEXT,
    -- Hull
    year_built              INTEGER,
    builder                 TEXT,
    loa_m                   FLOAT,
    beam_m                  FLOAT,
    draft_m                 FLOAT,
    hull_material           TEXT,
    -- Propulsion
    engine_manufacturer     TEXT,
    engine_model            TEXT,
    engine_type             TEXT,
    engine_power_kw         FLOAT,
    engine_serial           TEXT,
    engine_year             INTEGER,
    num_engines             INTEGER DEFAULT 1,
    -- Electrical
    battery_capacity_ah     FLOAT,
    battery_type            TEXT,
    -- NMEA auto-detected (from PGN 126996 Product Information)
    nmea_product_name       TEXT,
    nmea_manufacturer_code  TEXT,
    nmea_model_id           TEXT,
    nmea_software_version   TEXT,
    nmea_serial_number      TEXT,
    -- Meta
    first_seen              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),
    notes                   TEXT
);
