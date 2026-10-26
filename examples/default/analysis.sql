-- Standard SQL: exact full-table profiling. Read-only; review estimated bytes before execution.
-- No identifier samples are returned. Fanout counts precede filtering and deduplication.
WITH raw_values AS (
SELECT CASE WHEN LOWER(TRIM(CAST(`maid` AS STRING))) IN ('','null','none','n/a','na','unknown','undefined','-') THEN NULL ELSE TRIM(CAST(`maid` AS STRING)) END AS maid,
  CASE WHEN LOWER(TRIM(CAST(`ip` AS STRING))) IN ('','null','none','n/a','na','unknown','undefined','-') THEN NULL ELSE TRIM(CAST(`ip` AS STRING)) END AS ip,
  CASE WHEN LOWER(TRIM(CAST(`email` AS STRING))) IN ('','null','none','n/a','na','unknown','undefined','-') THEN NULL ELSE TRIM(CAST(`email` AS STRING)) END AS email,
  CASE WHEN LOWER(TRIM(CAST(`device_type` AS STRING))) IN ('','null','none','n/a','na','unknown','undefined','-') THEN NULL ELSE TRIM(CAST(`device_type` AS STRING)) END AS device_type,
  CASE WHEN LOWER(TRIM(CAST(`device_os` AS STRING))) IN ('','null','none','n/a','na','unknown','undefined','-') THEN NULL ELSE TRIM(CAST(`device_os` AS STRING)) END AS device_os,
  CASE WHEN LOWER(TRIM(CAST(`timestamp` AS STRING))) IN ('','null','none','n/a','na','unknown','undefined','-') THEN NULL ELSE TRIM(CAST(`timestamp` AS STRING)) END AS timestamp FROM `my_project.raw.source_events`
),
normalized AS (
SELECT CASE WHEN REGEXP_CONTAINS(maid, r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$") AND LOWER(maid) <> '00000000-0000-0000-0000-000000000000' THEN LOWER(maid) END AS maid, maid AS raw_maid,
  NET.IP_TO_STRING(NET.SAFE_IP_FROM_STRING(ip)) AS ip, ip AS raw_ip,
  CASE WHEN LENGTH(email) <= 254 AND REGEXP_CONTAINS(email, r"^[^ @]+@[^ @.]+([.][^ @.]+)+$") AND NOT REGEXP_CONTAINS(email, r"\s") THEN LOWER(email) END AS email, email AS raw_email,
  CASE LOWER(device_type) WHEN 'phone' THEN 'mobile' WHEN 'smartphone' THEN 'mobile' WHEN 'mobile' THEN 'mobile' WHEN 'tablet' THEN 'tablet' WHEN 'ipad' THEN 'tablet' WHEN 'desktop' THEN 'desktop' WHEN 'pc' THEN 'desktop' WHEN 'laptop' THEN 'desktop' WHEN 'tv' THEN 'tv' WHEN 'smart tv' THEN 'tv' END AS device_type, device_type AS raw_device_type,
  CASE LOWER(device_os) WHEN 'iphone' THEN 'ios' WHEN 'ipad' THEN 'ipados' WHEN 'ipad os' THEN 'ipados' WHEN 'ipados' THEN 'ipados' WHEN 'ios' THEN 'ios' WHEN 'android' THEN 'android' WHEN 'windows' THEN 'windows' WHEN 'win' THEN 'windows' WHEN 'mac' THEN 'macos' WHEN 'mac os' THEN 'macos' WHEN 'osx' THEN 'macos' WHEN 'macos' THEN 'macos' WHEN 'linux' THEN 'linux' END AS device_os, device_os AS raw_device_os,
  CASE WHEN SAFE_CAST(timestamp AS FLOAT64) IS NOT NULL THEN SAFE.TIMESTAMP_MICROS(SAFE_CAST(SAFE_CAST(timestamp AS FLOAT64) * (CASE WHEN ABS(SAFE_CAST(timestamp AS FLOAT64)) < 1e11 THEN 1000000 WHEN ABS(SAFE_CAST(timestamp AS FLOAT64)) < 1e14 THEN 1000 WHEN ABS(SAFE_CAST(timestamp AS FLOAT64)) < 1e17 THEN 1 ELSE 0.001 END) AS INT64)) ELSE SAFE_CAST(timestamp AS TIMESTAMP) END AS timestamp, timestamp AS raw_timestamp FROM raw_values
),
checked AS (
SELECT maid, ip, email, CASE WHEN COALESCE(((device_os = 'ios' AND device_type NOT IN ('mobile','tablet')) OR (device_os = 'ipados' AND device_type NOT IN ('tablet')) OR (device_os = 'android' AND device_type NOT IN ('mobile','tablet','tv')) OR (device_os = 'windows' AND device_type NOT IN ('desktop','tablet')) OR (device_os = 'macos' AND device_type NOT IN ('desktop')) OR (device_os = 'linux' AND device_type NOT IN ('desktop'))), FALSE) THEN device_type ELSE device_type END AS device_type, device_os, timestamp, raw_maid IS NOT NULL AND maid IS NULL AS invalid_maid,
  raw_ip IS NOT NULL AND ip IS NULL AS invalid_ip,
  raw_email IS NOT NULL AND email IS NULL AS invalid_email,
  raw_device_type IS NOT NULL AND device_type IS NULL AS invalid_device_type,
  raw_device_os IS NOT NULL AND device_os IS NULL AS invalid_device_os,
  timestamp IS NULL AS invalid_timestamp,
  COALESCE(((device_os = 'ios' AND device_type NOT IN ('mobile','tablet')) OR (device_os = 'ipados' AND device_type NOT IN ('tablet')) OR (device_os = 'android' AND device_type NOT IN ('mobile','tablet','tv')) OR (device_os = 'windows' AND device_type NOT IN ('desktop','tablet')) OR (device_os = 'macos' AND device_type NOT IN ('desktop')) OR (device_os = 'linux' AND device_type NOT IN ('desktop'))), FALSE) AND NOT (FALSE) AS device_os_type_mismatch,
  COALESCE(((device_os = 'ios' AND device_type NOT IN ('mobile','tablet')) OR (device_os = 'ipados' AND device_type NOT IN ('tablet')) OR (device_os = 'android' AND device_type NOT IN ('mobile','tablet','tv')) OR (device_os = 'windows' AND device_type NOT IN ('desktop','tablet')) OR (device_os = 'macos' AND device_type NOT IN ('desktop')) OR (device_os = 'linux' AND device_type NOT IN ('desktop'))), FALSE) AND (FALSE) AS repaired_device_type,
  maid IS NULL AND ip IS NULL AND email IS NULL AS missing_identifier FROM normalized
),
runner_0 AS (
SELECT maid, COUNT(DISTINCT ip) AS fanout FROM checked WHERE maid IS NOT NULL GROUP BY maid
),
runner_1 AS (
SELECT ip, COUNT(DISTINCT maid) AS fanout FROM checked WHERE ip IS NOT NULL GROUP BY ip
),
runner_2 AS (
SELECT email, COUNT(DISTINCT maid) AS fanout FROM checked WHERE email IS NOT NULL GROUP BY email
),
flagged AS (
SELECT c.*, COALESCE(r0.fanout > 1000, FALSE) AS high_ips_per_maid, COALESCE(r1.fanout > 20000, FALSE) AS high_maids_per_ip, COALESCE(r2.fanout > 30000, FALSE) AS high_maids_per_email FROM checked c
LEFT JOIN runner_0 r0 ON c.maid = r0.maid
LEFT JOIN runner_1 r1 ON c.ip = r1.ip
LEFT JOIN runner_2 r2 ON c.email = r2.email
)
SELECT COUNT(*) AS total_rows,
 COUNTIF(NOT (invalid_maid OR invalid_ip OR invalid_email OR invalid_device_type OR invalid_device_os OR invalid_timestamp OR device_os_type_mismatch OR missing_identifier OR high_ips_per_maid OR high_maids_per_ip OR high_maids_per_email)) AS valid_rows_before_deduplication,
 COUNTIF(repaired_device_type) AS repairable_device_rows,
 [STRUCT('maid' AS field, (SELECT COUNT(DISTINCT maid) FROM raw_values) AS distinct_values, (SELECT COUNTIF(maid IS NULL) FROM raw_values) AS null_count, (SELECT COUNT(DISTINCT maid) FROM normalized) AS normalized_distinct_values),
STRUCT('ip' AS field, (SELECT COUNT(DISTINCT ip) FROM raw_values) AS distinct_values, (SELECT COUNTIF(ip IS NULL) FROM raw_values) AS null_count, (SELECT COUNT(DISTINCT ip) FROM normalized) AS normalized_distinct_values),
STRUCT('email' AS field, (SELECT COUNT(DISTINCT email) FROM raw_values) AS distinct_values, (SELECT COUNTIF(email IS NULL) FROM raw_values) AS null_count, (SELECT COUNT(DISTINCT email) FROM normalized) AS normalized_distinct_values),
STRUCT('device_type' AS field, (SELECT COUNT(DISTINCT device_type) FROM raw_values) AS distinct_values, (SELECT COUNTIF(device_type IS NULL) FROM raw_values) AS null_count, (SELECT COUNT(DISTINCT device_type) FROM normalized) AS normalized_distinct_values),
STRUCT('device_os' AS field, (SELECT COUNT(DISTINCT device_os) FROM raw_values) AS distinct_values, (SELECT COUNTIF(device_os IS NULL) FROM raw_values) AS null_count, (SELECT COUNT(DISTINCT device_os) FROM normalized) AS normalized_distinct_values),
STRUCT('timestamp' AS field, (SELECT COUNT(DISTINCT timestamp) FROM raw_values) AS distinct_values, (SELECT COUNTIF(timestamp IS NULL) FROM raw_values) AS null_count, (SELECT COUNT(DISTINCT timestamp) FROM normalized) AS normalized_distinct_values)] AS columns,
 [STRUCT('invalid_maid' AS rule, COUNTIF(invalid_maid) AS affected_rows),
STRUCT('invalid_ip' AS rule, COUNTIF(invalid_ip) AS affected_rows),
STRUCT('invalid_email' AS rule, COUNTIF(invalid_email) AS affected_rows),
STRUCT('invalid_device_type' AS rule, COUNTIF(invalid_device_type) AS affected_rows),
STRUCT('invalid_device_os' AS rule, COUNTIF(invalid_device_os) AS affected_rows),
STRUCT('invalid_timestamp' AS rule, COUNTIF(invalid_timestamp) AS affected_rows),
STRUCT('device_os_type_mismatch' AS rule, COUNTIF(device_os_type_mismatch) AS affected_rows),
STRUCT('missing_identifier' AS rule, COUNTIF(missing_identifier) AS affected_rows),
STRUCT('high_ips_per_maid' AS rule, COUNTIF(high_ips_per_maid) AS affected_rows),
STRUCT('high_maids_per_ip' AS rule, COUNTIF(high_maids_per_ip) AS affected_rows),
STRUCT('high_maids_per_email' AS rule, COUNTIF(high_maids_per_email) AS affected_rows)] AS issues
FROM flagged;
