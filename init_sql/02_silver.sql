CREATE TABLE silver.request_cleaned (
    request_id       BIGINT,
    created_date     TIMESTAMP,
    closed_date      TIMESTAMP,
    agency_code      TEXT,
    agency_name      TEXT,
    complaint_type   TEXT,
    descriptor       TEXT,
    borough          TEXT,
    city             TEXT,
    incident_zip     TEXT,
    incident_address TEXT,
    status           TEXT,
    channel          TEXT,
    latitude         NUMERIC,
    longitude        NUMERIC
);