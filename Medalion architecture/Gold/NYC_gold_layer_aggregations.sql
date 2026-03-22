CREATE TABLE gold.aggregation_complaint_by_year AS
SELECT
    c.category,
    c.complaint_type,
    d.year,
    COUNT(*)                AS request_count,
    ROUND(AVG(r.resolution_hours) FILTER (
        WHERE r.resolution_hours > 0), 1)       AS avg_resolution_hours
FROM gold.requests r
JOIN gold.complaint c ON r.complaint_key = c.complaint_key
JOIN gold.date      d ON r.date_key      = d.date_key
GROUP BY c.category, c.complaint_type, d.year
ORDER BY d.year, request_count DESC;

CREATE TABLE gold.aggregation_channel_by_borough AS
SELECT
    l.borough,
    r.channel,
    COUNT(*)          AS request_count,
    ROUND(
        100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY l.borough), 1
    )                 AS pct_of_borough_total
FROM gold.requests r
JOIN gold.location l ON r.location_key = l.location_key
WHERE r.channel IS NOT NULL
  AND l.borough IS NOT NULL
GROUP BY l.borough, r.channel
ORDER BY l.borough, request_count DESC;
