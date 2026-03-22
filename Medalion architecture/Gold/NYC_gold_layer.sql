CREATE SCHEMA gold;

CREATE TABLE gold.date AS
SELECT DISTINCT
    TO_CHAR(created_date, 'YYYYMMDD')::INT                      AS date_key,
    created_date::DATE                                           AS full_date,
    EXTRACT(YEAR  FROM created_date)::INT                        AS year,
    EXTRACT(MONTH FROM created_date)::INT                        AS month,
    EXTRACT(QUARTER FROM created_date)::INT                      AS quarter,
    TO_CHAR(created_date, 'Day')                                 AS day_of_week,
    CASE WHEN EXTRACT(DOW FROM created_date) IN (0, 6)
         THEN TRUE ELSE FALSE END                                AS is_weekend,
    CASE
        WHEN EXTRACT(MONTH FROM created_date) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM created_date) IN (3,  4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM created_date) IN (6,  7, 8) THEN 'Summer'
        ELSE 'Fall'
    END                                                          AS season
FROM silver.request_cleaned;

CREATE TABLE gold.agency AS
SELECT
    ROW_NUMBER() OVER (ORDER BY agency_code)    AS agency_key,
    agency_code,
    agency_name,
    CASE agency_code
        WHEN 'NYPD'  THEN 'Public Safety'
        WHEN 'FIRE'  THEN 'Public Safety'
        WHEN 'DOC'   THEN 'Public Safety'
        WHEN 'DOT'   THEN 'Infrastructure'
        WHEN 'DEP'   THEN 'Infrastructure'
        WHEN 'DSNY'  THEN 'Infrastructure'
        WHEN 'HPD'   THEN 'Housing'
        WHEN 'DOB'   THEN 'Housing'
        WHEN 'DCA'   THEN 'Consumer Affairs'
        WHEN 'DOHMH' THEN 'Health'
        WHEN 'DPR'   THEN 'Parks'
        WHEN 'TLC'   THEN 'Transportation'
        ELSE              'Other City Agency'
    END                                         AS agency_type
FROM (
    SELECT DISTINCT agency_code, agency_name
    FROM silver.request_cleaned
);
 
CREATE TABLE gold.location AS
SELECT
    ROW_NUMBER() OVER (ORDER BY borough, incident_zip)  AS location_key,
    borough,
    incident_zip,
    city,
    AVG(latitude)                                        AS latitude,
    AVG(longitude)                                       AS longitude
FROM silver.request_cleaned
WHERE borough IS NOT NULL
  AND borough != 'UNSPECIFIED'
GROUP BY borough, incident_zip, city;
 
DROP TABLE IF EXISTS gold.complaint;

CREATE TABLE gold.complaint AS
SELECT
    ROW_NUMBER() OVER (ORDER BY complaint_type) AS complaint_key,
    complaint_type,
    CASE
        WHEN complaint_type ILIKE '%noise%'    THEN 'Noise'
        WHEN complaint_type ILIKE '%heat%'
          OR complaint_type ILIKE '%plumbing%'
          OR complaint_type ILIKE '%paint%'
          OR complaint_type ILIKE '%mold%'     THEN 'Housing Maintenance'
        WHEN complaint_type ILIKE '%parking%'
          OR complaint_type ILIKE '%traffic%'
          OR complaint_type ILIKE '%bike%'     THEN 'Transportation'
        WHEN complaint_type ILIKE '%rodent%'
          OR complaint_type ILIKE '%sanit%'
          OR complaint_type ILIKE '%dirty%'
          OR complaint_type ILIKE '%trash%'    THEN 'Sanitation'
        WHEN complaint_type ILIKE '%drug%'
          OR complaint_type ILIKE '%assault%'
          OR complaint_type ILIKE '%illegal%'  THEN 'Public Safety'
        WHEN complaint_type ILIKE '%tree%'
          OR complaint_type ILIKE '%park%'     THEN 'Parks'
        WHEN complaint_type ILIKE '%water%'
          OR complaint_type ILIKE '%sewer%'    THEN 'Water & Sewer'
        ELSE 'Other'
    END AS category
FROM (
    SELECT DISTINCT complaint_type
    FROM silver.request_cleaned
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
(6, 'UNSPECIFIED', 'Unspecified', FALSE, FALSE);

CREATE INDEX ON gold.agency    (agency_code);
CREATE INDEX ON gold.location  (borough, incident_zip);
CREATE INDEX ON gold.complaint (complaint_type);
CREATE INDEX ON gold.status    (status_code);

CREATE TABLE gold.requests AS
SELECT
    s.request_id,
    TO_CHAR(s.created_date, 'YYYYMMDD')::INT   AS date_key,
    a.agency_key,
    l.location_key,
    c.complaint_key,
    st.status_key,
    ROUND(
        EXTRACT(EPOCH FROM (s.closed_date - s.created_date)) / 3600.0
    )::INT                                      AS resolution_hours
FROM silver.request_cleaned s
LEFT JOIN gold.agency    a  ON s.agency_code   = a.agency_code
LEFT JOIN gold.location  l  ON s.borough       = l.borough
                           AND COALESCE(s.incident_zip, 'N/A') 
                             = COALESCE(l.incident_zip, 'N/A')
LEFT JOIN gold.complaint c  ON s.complaint_type = c.complaint_type
LEFT JOIN gold.status    st ON s.status         = st.status_code;