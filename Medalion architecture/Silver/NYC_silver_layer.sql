{\rtf1\ansi\ansicpg1250\cocoartf2822
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 CREATE SCHEMA silver;\
\
CREATE TABLE silver.request_cleaned AS\
SELECT DISTINCT ON (unique_key)\
    unique_key::BIGINT                                                      AS request_id,\
    TO_TIMESTAMP(created_date, 'MM/DD/YYYY HH12:MI:SS AM')                 AS created_date,\
    TO_TIMESTAMP(closed_date,  'MM/DD/YYYY HH12:MI:SS AM')                 AS closed_date,\
    UPPER(TRIM(agency))                                                     AS agency_code,\
    INITCAP(TRIM(agency_name))                                              AS agency_name,\
    INITCAP(TRIM(problem))                                                  AS complaint_type,\
    INITCAP(TRIM(problem_detail))                                           AS descriptor,\
    UPPER(TRIM(borough))                                                    AS borough,\
    INITCAP(TRIM(city))                                                     AS city,\
    NULLIF(TRIM(incident_zip), '')                                          AS incident_zip,\
    INITCAP(TRIM(incident_address))                                         AS incident_address,\
    UPPER(TRIM(status))                                                     AS status,\
    UPPER(TRIM(open_data_channel_type))                                     AS channel,\
    NULLIF(latitude::NUMERIC,  0)                                           AS latitude,\
    NULLIF(longitude::NUMERIC, 0)                                           AS longitude\
FROM bronze.request_raw\
WHERE unique_key IS NOT NULL\
  AND created_date IS NOT NULL\
  AND (\
      closed_date IS NULL\
      OR TO_TIMESTAMP(closed_date, 'MM/DD/YYYY HH12:MI:SS AM')\
       > TO_TIMESTAMP(created_date, 'MM/DD/YYYY HH12:MI:SS AM')\
  	)\
ORDER BY unique_key, created_date;}