CREATE TABLE gold.date (
    date_key    INT,
    full_date   DATE,
    year        INT,
    month       INT,
    quarter     INT,
    day_of_week TEXT,
    is_weekend  BOOLEAN,
    season      TEXT
);

CREATE TABLE gold.agency (
    agency_key  BIGINT,
    agency_code TEXT,
    agency_name TEXT,
    agency_type TEXT
);

CREATE TABLE gold.location (
    location_key BIGINT,
    borough      TEXT,
    incident_zip TEXT,
    city         TEXT,
    latitude     NUMERIC,
    longitude    NUMERIC
);

CREATE TABLE gold.complaint (
    complaint_key BIGINT,
    complaint_type TEXT,
    category       TEXT
);

CREATE TABLE gold.status (
    status_key   INT PRIMARY KEY,
    status_code  VARCHAR(50),
    status_label VARCHAR(100),
    is_resolved  BOOLEAN,
    is_pending   BOOLEAN
);

INSERT INTO gold.status VALUES
(1, 'CLOSED',      'Closed',      TRUE,  FALSE),
(2, 'OPEN',        'Open',        FALSE, TRUE),
(3, 'PENDING',     'Pending',     FALSE, TRUE),
(4, 'IN PROGRESS', 'In Progress', FALSE, TRUE),
(5, 'ASSIGNED',    'Assigned',    FALSE, TRUE),
(6, 'UNSPECIFIED', 'Unspecified', FALSE, FALSE)
ON CONFLICT DO NOTHING;

CREATE TABLE gold.requests (
    request_id       BIGINT,
    date_key         INT,
    agency_key       BIGINT,
    location_key     BIGINT,
    complaint_key    BIGINT,
    status_key       INT,
    resolution_hours INT
);

CREATE TABLE gold.aggregation_complaint_by_year (
    category             TEXT,
    complaint_type       TEXT,
    year                 INT,
    request_count        BIGINT,
    avg_resolution_hours NUMERIC
);

CREATE TABLE gold.aggregation_channel_by_borough (
    borough              TEXT,
    channel              TEXT,
    request_count        BIGINT,
    pct_of_borough_total NUMERIC
);