/*
============================================================
  File: vbm_hospital_kpis.sql
  Project: Verulam Blue Mint — Healthcare Encounters (C08_l01)
  Source: The TEMP VIEW gold_encounters_final
  Author: Verulam Blue

  Output contract:
    Each KPI returns:
      (kpi_name, kpi_value, kpi_key)

    Final output:
      kpi_results = UNION ALL of kpi_1 ... kpi_10
============================================================
*/


-- ============================================================
-- KPI 1 — Total Encounters
-- ============================================================
CREATE OR REPLACE TEMP VIEW kpi_1 AS
SELECT
  'kpi_1' AS kpi_name,
  CAST(COUNT(*) AS VARCHAR) AS kpi_value,
  CAST(NULL AS VARCHAR)     AS kpi_key
FROM gold_encounters_final;


-- ============================================================
-- KPI 2 — Admission Rate
-- ============================================================
CREATE OR REPLACE TEMP VIEW kpi_2 AS
SELECT
  'kpi_2' AS kpi_name,
  CAST(
    ROUND(
      SUM(CASE WHEN is_admitted = 1 THEN 1 ELSE 0 END) * 1.0
      / NULLIF(COUNT(*), 0),
      6
    ) AS VARCHAR
  ) AS kpi_value,
  CAST(NULL AS VARCHAR) AS kpi_key
FROM gold_encounters_final;


-- ============================================================
-- KPI 3 — Average LOS (Admitted + LOS-eligible)
-- ============================================================
CREATE OR REPLACE TEMP VIEW kpi_3 AS
SELECT
  'kpi_3' AS kpi_name,
  CAST(
    ROUND(
      AVG(length_of_stay_days),
      3
    ) AS VARCHAR
  ) AS kpi_value,
  CAST(NULL AS VARCHAR) AS kpi_key
FROM gold_encounters_final
WHERE is_admitted = 1
  AND los_eligible_flag = 1;


-- ============================================================
-- KPI 4 — Median LOS (Admitted + LOS-eligible)
-- ============================================================
CREATE OR REPLACE TEMP VIEW kpi_4 AS
SELECT
  'kpi_4' AS kpi_name,
  CAST(
    ROUND(
      MEDIAN(length_of_stay_days),
      3
    ) AS VARCHAR
  ) AS kpi_value,
  CAST(NULL AS VARCHAR) AS kpi_key
FROM gold_encounters_final
WHERE is_admitted = 1
  AND los_eligible_flag = 1;


-- ============================================================
-- KPI 5 — High-Acuity Share
-- ============================================================
CREATE OR REPLACE TEMP VIEW kpi_5 AS
SELECT
  'kpi_5' AS kpi_name,
  CAST(
    ROUND(
      SUM(CASE WHEN severity_level IN ('High', 'Critical') THEN 1 ELSE 0 END) * 1.0
      / NULLIF(COUNT(*), 0),
      6
    ) AS VARCHAR
  ) AS kpi_value,
  CAST(NULL AS VARCHAR) AS kpi_key
FROM gold_encounters_final;


-- ============================================================
-- KPI 6 — Emergency Encounter Share
-- ============================================================
CREATE OR REPLACE TEMP VIEW kpi_6 AS
SELECT
  'kpi_6' AS kpi_name,
  CAST(
    ROUND(
      SUM(CASE WHEN encounter_type = 'Emergency' THEN 1 ELSE 0 END) * 1.0
      / NULLIF(COUNT(*), 0),
      6
    ) AS VARCHAR
  ) AS kpi_value,
  CAST(NULL AS VARCHAR) AS kpi_key
FROM gold_encounters_final;


-- ============================================================
-- KPI 7 — Department with Highest Average LOS
-- ============================================================
CREATE OR REPLACE TEMP VIEW kpi_7 AS
WITH dept_los AS (
  SELECT
    department,
    AVG(length_of_stay_days) AS avg_los_days
  FROM gold_encounters_final
  WHERE is_admitted = 1
    AND los_eligible_flag = 1
  GROUP BY department
)
SELECT
  'kpi_7' AS kpi_name,
  CAST(ROUND(avg_los_days, 3) AS VARCHAR) AS kpi_value,
  CAST(department AS VARCHAR)             AS kpi_key
FROM dept_los
ORDER BY avg_los_days DESC, department
LIMIT 1;


-- ============================================================
-- KPI 8 — Busiest Department by Volume (Share of Total)
-- ============================================================
CREATE OR REPLACE TEMP VIEW kpi_8 AS
WITH dept_counts AS (
  SELECT
    department,
    COUNT(*) AS dept_encounters
  FROM gold_encounters_final
  GROUP BY department
),
tot AS (
  SELECT SUM(dept_encounters) AS total_encounters
  FROM dept_counts
),
ranked AS (
  SELECT
    d.department,
    d.dept_encounters,
    d.dept_encounters * 1.0 / NULLIF(t.total_encounters, 0) AS dept_share,
    ROW_NUMBER() OVER (ORDER BY d.dept_encounters DESC, d.department) AS rn
  FROM dept_counts d
  CROSS JOIN tot t
)
SELECT
  'kpi_8' AS kpi_name,
  CAST(ROUND(dept_share, 6) AS VARCHAR) AS kpi_value,
  CAST(department AS VARCHAR)           AS kpi_key
FROM ranked
WHERE rn = 1;


-- ============================================================
-- KPI 9 — Top 3 Primary Diagnoses (3 rows)
-- ============================================================
CREATE OR REPLACE TEMP VIEW kpi_9 AS
WITH diag_counts AS (
  SELECT
    primary_diagnosis,
    COUNT(*) AS encounter_count
  FROM gold_encounters_final
  GROUP BY primary_diagnosis
)
SELECT
  'kpi_9' AS kpi_name,
  CAST(encounter_count AS VARCHAR)   AS kpi_value,
  CAST(primary_diagnosis AS VARCHAR) AS kpi_key
FROM diag_counts
ORDER BY encounter_count DESC, primary_diagnosis
LIMIT 3;


-- ============================================================
-- KPI 10 — 30-Day Readmission Rate
-- ============================================================
CREATE OR REPLACE TEMP VIEW kpi_10 AS
WITH admitted AS (
  SELECT
    encounter_id,
    patient_id,
    admission_date,
    discharge_date
  FROM gold_encounters_final
  WHERE is_admitted = 1
    AND readmission_eligible_flag = 1
),
sequenced AS (
  SELECT
    *,
    LAG(discharge_date) OVER (
      PARTITION BY patient_id
      ORDER BY admission_date
    ) AS prev_discharge_date
  FROM admitted
),
flagged AS (
  SELECT
    encounter_id,
    CASE
      WHEN prev_discharge_date IS NULL THEN 0
      WHEN DATEDIFF('day', prev_discharge_date, admission_date) BETWEEN 0 AND 30 THEN 1
      ELSE 0
    END AS is_30d_readmission
  FROM sequenced
)
SELECT
  'kpi_10' AS kpi_name,
  CAST(ROUND(AVG(CAST(is_30d_readmission AS DOUBLE)), 6) AS VARCHAR) AS kpi_value,
  CAST(NULL AS VARCHAR) AS kpi_key
FROM flagged;


-- ============================================================
-- FINAL — Consolidated KPI Results
-- ============================================================
CREATE OR REPLACE TABLE kpi_results AS
SELECT * FROM kpi_1
UNION ALL SELECT * FROM kpi_2
UNION ALL SELECT * FROM kpi_3
UNION ALL SELECT * FROM kpi_4
UNION ALL SELECT * FROM kpi_5
UNION ALL SELECT * FROM kpi_6
UNION ALL SELECT * FROM kpi_7
UNION ALL SELECT * FROM kpi_8
UNION ALL SELECT * FROM kpi_9
UNION ALL SELECT * FROM kpi_10;
