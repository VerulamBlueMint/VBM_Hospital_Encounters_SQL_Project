/*
============================================================
  File: vbm_hospital_data_cleaning_pipeline.sql
  Project: Verulam Blue Mint — Healthcare Encounters (C08_l01)
  Dataset: C08_l01_healthcare_encounters_data_table
  Author: Verulam Blue

  Description:
    Cleans raw encounter records into two published views:

    1) silver_encounters_final
       - Fully cleaned dataset (all years)
       - Deduplicated
       - Dates parsed and validated
       - Categoricals normalised + placeholders imputed
       - Deterministic is_admitted derived from encounter_type
       - LOS recomputed from admission/discharge where valid
       - encounter_id made non-null via deterministic synthetic IDs

    2) gold_encounters_final
       - Analysis-ready subset for KPIs (2025 only)
       - Includes eligibility flags + helper flags for KPI filtering
============================================================
*/

-- =========================================================
-- PHASE A — STANDARDISATION (Steps 1–2)
-- =========================================================

/* ---------------------------------------------------------
   Step 1 — Add technical tracer row id (bronze_row_id)
--------------------------------------------------------- */
CREATE OR REPLACE TEMP VIEW encounters_bronze_rowid AS
SELECT
  ROW_NUMBER() OVER (
    ORDER BY
      patient_id,
      encounter_date,
      admission_date,
      discharge_date
  ) AS bronze_row_id,
  *
FROM C08_l01_healthcare_encounters_data_table;

/* ---------------------------------------------------------
   Step 2 — Standardise to *_txt (TRIM, blank→NULL, lower)
--------------------------------------------------------- */
CREATE OR REPLACE TEMP VIEW encounters_silver_standard_txt AS
SELECT
  bronze_row_id,

  -- IDs and keys
  NULLIF(LOWER(TRIM(CAST(encounter_id AS VARCHAR))), '') AS encounter_id_txt,
  NULLIF(LOWER(TRIM(CAST(patient_id   AS VARCHAR))), '') AS patient_id_txt,

  -- dates: trim + blank→NULL (no LOWER)
  NULLIF(TRIM(CAST(encounter_date   AS VARCHAR)), '') AS encounter_date_txt,
  NULLIF(TRIM(CAST(admission_date   AS VARCHAR)), '') AS admission_date_txt,
  NULLIF(TRIM(CAST(discharge_date   AS VARCHAR)), '') AS discharge_date_txt,

  -- categoricals (lower for matching)
  NULLIF(LOWER(TRIM(CAST(encounter_type    AS VARCHAR))), '') AS encounter_type_txt,
  NULLIF(LOWER(TRIM(CAST(department        AS VARCHAR))), '') AS department_txt,
  NULLIF(LOWER(TRIM(CAST(primary_diagnosis AS VARCHAR))), '') AS primary_diagnosis_txt,
  NULLIF(LOWER(TRIM(CAST(severity_level    AS VARCHAR))), '') AS severity_level_txt,

  -- admission flag as text
  NULLIF(LOWER(TRIM(CAST(is_admitted AS VARCHAR))), '') AS is_admitted_txt,

  -- LOS as text for later validation/cast
  NULLIF(TRIM(CAST(length_of_stay_days AS VARCHAR)), '') AS length_of_stay_days_txt
FROM encounters_bronze_rowid;

-- =========================================================
-- PHASE B — DATE CHECKS (Step 4)
-- =========================================================

/* ---------------------------------------------------------
   Step 4 — Parse dates safely + validate spell order
   - TRY_STRPTIME returns NULL on parse failure
   - Filter out rows with missing discharge_date
   - Enforce discharge >= admission when both parse
--------------------------------------------------------- */

-- 4.1 Parse + flags (keep everything)
CREATE OR REPLACE TEMP VIEW encounters_silver_parsed_dates_flagged AS
SELECT
  bronze_row_id,
  encounter_id_txt,
  patient_id_txt,

  -- date text
  encounter_date_txt,
  admission_date_txt,
  discharge_date_txt,

  -- parsed dates
  CAST(TRY_STRPTIME(encounter_date_txt,  '%d-%m-%Y') AS DATE) AS encounter_date_parsed,
  CAST(TRY_STRPTIME(admission_date_txt,  '%d-%m-%Y') AS DATE) AS admission_date_parsed,
  CAST(TRY_STRPTIME(discharge_date_txt, '%d-%m-%Y') AS DATE) AS discharge_date_parsed,

  -- flags
  CASE
    WHEN CAST(TRY_STRPTIME(discharge_date_txt, '%d-%m-%Y') AS DATE) IS NULL THEN 0
    ELSE 1
  END AS has_discharge_date,

  CASE
    WHEN CAST(TRY_STRPTIME(admission_date_txt, '%d-%m-%Y') AS DATE) IS NULL THEN 1
    WHEN CAST(TRY_STRPTIME(discharge_date_txt, '%d-%m-%Y') AS DATE) IS NULL THEN 0
    WHEN CAST(TRY_STRPTIME(discharge_date_txt, '%d-%m-%Y') AS DATE)
       >= CAST(TRY_STRPTIME(admission_date_txt,  '%d-%m-%Y') AS DATE) THEN 1
    ELSE 0
  END AS is_discharge_on_or_after_admission,

  -- categoricals
  encounter_type_txt,
  department_txt,
  primary_diagnosis_txt,
  severity_level_txt,

  -- flags + LOS
  is_admitted_txt,
  length_of_stay_days_txt
FROM encounters_silver_standard_txt;

-- 4.2 Parsed-and-passed only
CREATE OR REPLACE TEMP VIEW encounters_silver_parsed_dates AS
SELECT
  bronze_row_id,
  encounter_id_txt,
  patient_id_txt,

  encounter_date_parsed,
  admission_date_parsed,
  discharge_date_parsed,

  encounter_type_txt,
  department_txt,
  primary_diagnosis_txt,
  severity_level_txt,

  is_admitted_txt,
  length_of_stay_days_txt
FROM encounters_silver_parsed_dates_flagged
WHERE has_discharge_date = 1
  AND is_discharge_on_or_after_admission = 1;


-- =========================================================
-- PHASE C — NORMALISATION & RULES (Steps 5–6)
-- =========================================================

/* ---------------------------------------------------------
   Step 5 — Normalise categoricals via mapping views
--------------------------------------------------------- */

-- Encounter type mapping
CREATE OR REPLACE TEMP VIEW map_encounter_type AS
WITH mapping_data(raw_value, canonical_value) AS (
  VALUES
    ('emerg.', 'Emergency'),
    ('emergancy', 'Emergency'),
    ('emergency', 'Emergency'),
    ('in patient', 'Inpatient'),
    ('in-patient', 'Inpatient'),
    ('inpatient', 'Inpatient'),
    ('obs', 'Observation'),
    ('observation', 'Observation'),
    ('out patient', 'Outpatient'),
    ('out-patient', 'Outpatient'),
    ('outpatient', 'Outpatient')
)
SELECT raw_value, canonical_value
FROM mapping_data;

CREATE OR REPLACE TEMP VIEW encounters_silver_norm_encounter_type AS
SELECT
  e.*,
  m.canonical_value AS encounter_type_clean
FROM encounters_silver_parsed_dates e
LEFT JOIN map_encounter_type m
  ON e.encounter_type_txt = m.raw_value;

-- Department mapping
CREATE OR REPLACE TEMP VIEW map_department AS
WITH mapping_data(raw_value, canonical_value) AS (
  VALUES
    ('a & e', 'A&E'),
    ('a&e', 'A&E'),
    ('accident & emergency', 'A&E'),
    ('accident_and_emergency', 'A&E'),
    ('ae', 'A&E'),
    ('cardiology', 'Cardiology'),
    ('critical_care', 'ICU'),
    ('i.c.u.', 'ICU'),
    ('icu', 'ICU'),
    ('gen medicine', 'General_Medicine'),
    ('gen_med', 'General_Medicine'),
    ('general medicine', 'General_Medicine'),
    ('general_medicine', 'General_Medicine'),
    ('general_surgery', 'Surgery'),
    ('surgery', 'Surgery'),
    ('paediatrics', 'Paediatrics'),
    ('paeds', 'Paediatrics'),
    ('t&o', 'Trauma_&_Orthopaedics'),
    ('trauma & orthopaedics', 'Trauma_&_Orthopaedics'),
    ('trauma_&_orthopaedics', 'Trauma_&_Orthopaedics'),
    ('trauma_and_orthopaedics', 'Trauma_&_Orthopaedics')
)
SELECT raw_value, canonical_value
FROM mapping_data;

CREATE OR REPLACE TEMP VIEW encounters_silver_norm_department AS
SELECT
  e.*,
  m.canonical_value AS department_clean
FROM encounters_silver_norm_encounter_type e
LEFT JOIN map_department m
  ON e.department_txt = m.raw_value;

-- Severity mapping
CREATE OR REPLACE TEMP VIEW map_severity AS
WITH mapping_data(raw_value, canonical_value) AS (
  VALUES
    ('crit',  'Critical'),
    ('critical', 'Critical'),
    ('high',  'High'),
    ('low',   'Low'),
    ('med',   'Medium'),
    ('medium','Medium')
)
SELECT raw_value, canonical_value
FROM mapping_data;

CREATE OR REPLACE TEMP VIEW encounters_silver_norm_severity AS
SELECT
  e.*,
  m.canonical_value AS severity_level_clean
FROM encounters_silver_norm_department e
LEFT JOIN map_severity m
  ON e.severity_level_txt = m.raw_value;

-- Diagnosis mapping
CREATE OR REPLACE TEMP VIEW map_primary_diagnosis AS
WITH mapping_data(raw_value, canonical_value) AS (
  VALUES
    ('chest pain', 'Chest_Pain'),
    ('chest_pain', 'Chest_Pain'),
    ('heart failure', 'Heart_Failure'),
    ('heart_failure', 'Heart_Failure'),
    ('pneumonia', 'Pneumonia'),
    ('pnumonia', 'Pneumonia'),
    ('hip fracture', 'Hip_Fracture'),
    ('hip_fracture', 'Hip_Fracture'),
    ('diabetees', 'Diabetes'),
    ('diabetes', 'Diabetes'),
    ('c.o.p.d', 'COPD'),
    ('copd', 'COPD'),
    ('infection', 'Infection'),
    ('stroke', 'Stroke'),
    ('cva', 'Stroke'),
    ('misc', 'Other'),
    ('other', 'Other'),
    ('other_condition', 'Other')
)
SELECT raw_value, canonical_value
FROM mapping_data;

CREATE OR REPLACE TEMP VIEW encounters_silver_norm_diagnosis AS
SELECT
  e.*,
  m.canonical_value AS primary_diagnosis_clean
FROM encounters_silver_norm_severity e
LEFT JOIN map_primary_diagnosis m
  ON e.primary_diagnosis_txt = m.raw_value;

-- is_admitted mapping (true/false strings)
CREATE OR REPLACE TEMP VIEW map_admitted AS
WITH mapping_data(raw_value, canonical_value) AS (
  VALUES
    ('false', 0),
    ('true',  1)
)
SELECT raw_value, canonical_value
FROM mapping_data;

CREATE OR REPLACE TEMP VIEW encounters_silver_norm_admitted AS
SELECT
  e.*,
  m.canonical_value AS is_admitted_clean
FROM encounters_silver_norm_diagnosis e
LEFT JOIN map_admitted m
  ON e.is_admitted_txt = m.raw_value;

-- Impute controlled placeholders so grouping is safe
CREATE OR REPLACE TEMP VIEW encounters_silver_flagged_categoricals AS
SELECT
  *,
  CASE WHEN department_clean IS NULL THEN 1 ELSE 0 END AS flag_missing_department,
  CASE WHEN severity_level_clean IS NULL THEN 1 ELSE 0 END AS flag_missing_severity,
  CASE WHEN encounter_type_clean IS NULL THEN 1 ELSE 0 END AS flag_missing_encounter_type,
  CASE WHEN primary_diagnosis_clean IS NULL THEN 1 ELSE 0 END AS flag_missing_diagnosis
FROM encounters_silver_norm_admitted;

CREATE OR REPLACE TEMP VIEW encounters_silver_impute_categoricals AS
SELECT
  *,
  COALESCE(department_clean, 'Unknown_Department')        AS department_final,
  COALESCE(severity_level_clean, 'Unknown_Severity')      AS severity_level_final,
  COALESCE(encounter_type_clean, 'Unknown_Encounter')     AS encounter_type_final,
  COALESCE(primary_diagnosis_clean, 'Unknown_Diagnosis')  AS primary_diagnosis_final
FROM encounters_silver_flagged_categoricals;

-- Minimal core working set
CREATE OR REPLACE TEMP VIEW encounters_silver_core AS
SELECT
  bronze_row_id,
  encounter_id_txt,
  patient_id_txt,
  encounter_date_parsed,
  admission_date_parsed,
  discharge_date_parsed,
  length_of_stay_days_txt,
  encounter_type_final,
  department_final,
  primary_diagnosis_final,
  severity_level_final,
  is_admitted_clean
FROM encounters_silver_impute_categoricals;


/* ---------------------------------------------------------
   Step 6 — LOS recompute + deterministic admission status
--------------------------------------------------------- */

-- LOS recompute from admission/discharge (when valid)
CREATE OR REPLACE TEMP VIEW encounters_silver_los AS
WITH base AS (
  SELECT
    *,
    TRY_CAST(length_of_stay_days_txt AS INTEGER) AS length_of_stay_days_int,
    CASE
      WHEN admission_date_parsed IS NOT NULL
       AND discharge_date_parsed IS NOT NULL
       AND discharge_date_parsed >= admission_date_parsed
      THEN DATEDIFF('day', admission_date_parsed, discharge_date_parsed)
      ELSE NULL
    END AS length_of_stay_days_calc
  FROM encounters_silver_core
)
SELECT
  *,
  length_of_stay_days_calc AS length_of_stay_days_final,
  CASE
    WHEN length_of_stay_days_int IS NULL
         AND length_of_stay_days_calc IS NOT NULL THEN 1
    WHEN length_of_stay_days_int IS NOT NULL
         AND length_of_stay_days_calc IS NOT NULL
         AND length_of_stay_days_int <> length_of_stay_days_calc THEN 1
    WHEN length_of_stay_days_int IS NULL
         AND length_of_stay_days_calc IS NULL THEN 1
    ELSE 0
  END AS flag_los_mismatch
FROM base;

-- Deterministic admission rule from encounter type
CREATE OR REPLACE TEMP VIEW encounters_silver_is_admitted_clean AS
SELECT
  *,
  CASE
    WHEN encounter_type_final IN ('Inpatient', 'Observation') THEN 1
    ELSE 0
  END AS is_admitted_final,
  CASE
    WHEN is_admitted_clean = 1 AND encounter_type_final IN ('Emergency', 'Outpatient') THEN 1
    WHEN is_admitted_clean = 0 AND encounter_type_final IN ('Inpatient', 'Observation') THEN 1
    ELSE 0
  END AS is_admitted_inconsistent_flag
FROM encounters_silver_los;

-- KPI eligibility flags (carry forward)
CREATE OR REPLACE TEMP VIEW encounters_silver_kpi_eligibility_flags AS
SELECT
  *,
  CASE
    WHEN admission_date_parsed IS NOT NULL
     AND discharge_date_parsed IS NOT NULL
     AND discharge_date_parsed >= admission_date_parsed
    THEN 1 ELSE 0
  END AS los_eligible_flag,
  CASE
    WHEN admission_date_parsed IS NOT NULL
     AND discharge_date_parsed IS NOT NULL
     AND discharge_date_parsed >= admission_date_parsed
     AND is_admitted_final = 1
    THEN 1 ELSE 0
  END AS readmission_eligible_flag
FROM encounters_silver_is_admitted_clean;

-- Whittle-down view used for dedup + ID handling
CREATE OR REPLACE TEMP VIEW encounters_silver_for_ids AS
SELECT
  bronze_row_id,
  encounter_id_txt,
  patient_id_txt,
  encounter_date_parsed,
  admission_date_parsed,
  discharge_date_parsed,
  encounter_type_final,
  department_final,
  primary_diagnosis_final,
  severity_level_final,
  is_admitted_final,
  length_of_stay_days_final,
  los_eligible_flag,
  readmission_eligible_flag
FROM encounters_silver_kpi_eligibility_flags;



-- =========================================================
-- PHASE D — DEDUPLICATION (Step 7)
-- =========================================================

/* ---------------------------------------------------------
   Step 7 — Flag now, filter next
   Duplicates = exact match across all key columns (NULLs match)
   Keep the lowest bronze_row_id
--------------------------------------------------------- */

CREATE OR REPLACE TEMP VIEW encounters_silver_dedup_flagged AS
WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        encounter_id_txt,
        patient_id_txt,
        encounter_date_parsed,
        admission_date_parsed,
        discharge_date_parsed,
        encounter_type_final,
        department_final,
        primary_diagnosis_final,
        severity_level_final,
        is_admitted_final,
        length_of_stay_days_final
      ORDER BY bronze_row_id
    ) AS rn
  FROM encounters_silver_for_ids
)
SELECT
  *,
  CASE WHEN rn = 1 THEN 0 ELSE 1 END AS duplicate_row_flag
FROM ranked;

CREATE OR REPLACE TEMP VIEW encounters_silver_dedup AS
SELECT
  bronze_row_id,
  encounter_id_txt,
  patient_id_txt,
  encounter_date_parsed,
  admission_date_parsed,
  discharge_date_parsed,
  encounter_type_final,
  department_final,
  primary_diagnosis_final,
  severity_level_final,
  is_admitted_final,
  length_of_stay_days_final,
  los_eligible_flag,
  readmission_eligible_flag
FROM encounters_silver_dedup_flagged
WHERE duplicate_row_flag = 0;



-- =========================================================
-- PHASE E — FINAL IDENTIFIERS & PUBLISH (Steps 8–11)
-- =========================================================

/* ---------------------------------------------------------
   Step 8 — Synthetic encounter_id for NULL IDs (after dedup)
   Pattern: patient_id + YYYYMMDD + optional 2-digit sequence
--------------------------------------------------------- */

CREATE OR REPLACE TEMP VIEW encounters_silver_id_flags AS
SELECT
  *,
  CASE WHEN encounter_id_txt IS NULL THEN 1 ELSE 0 END AS needs_synth_id_flag
FROM encounters_silver_dedup;

CREATE OR REPLACE TEMP VIEW encounters_silver_id_null_seq AS
SELECT
  *,
  ROW_NUMBER() OVER (
    PARTITION BY patient_id_txt, encounter_date_parsed
    ORDER BY bronze_row_id
  ) AS seq_for_patient_day,
  COUNT(*) OVER (
    PARTITION BY patient_id_txt, encounter_date_parsed
  ) AS cnt_for_patient_day
FROM encounters_silver_id_flags
WHERE needs_synth_id_flag = 1;

CREATE OR REPLACE TEMP VIEW encounters_silver_id_seq_joined AS
SELECT
  d.*,
  n.seq_for_patient_day,
  n.cnt_for_patient_day
FROM encounters_silver_id_flags d
LEFT JOIN encounters_silver_id_null_seq n
  ON d.bronze_row_id = n.bronze_row_id;

CREATE OR REPLACE TEMP VIEW encounters_silver_encounter_id_synth AS
SELECT
  bronze_row_id,
  encounter_id_txt,
  patient_id_txt,
  encounter_date_parsed,
  admission_date_parsed,
  discharge_date_parsed,
  encounter_type_final,
  department_final,
  primary_diagnosis_final,
  severity_level_final,
  is_admitted_final,
  length_of_stay_days_final,
  los_eligible_flag,
  readmission_eligible_flag,

  CASE
    WHEN encounter_id_txt IS NOT NULL THEN encounter_id_txt
    ELSE
      patient_id_txt
      || STRFTIME(encounter_date_parsed, '%Y%m%d')
      || CASE
           WHEN cnt_for_patient_day = 1 THEN ''
           ELSE LPAD(CAST(seq_for_patient_day AS VARCHAR), 2, '0')
         END
  END AS encounter_id_clean,

  needs_synth_id_flag AS encounter_id_was_synthetic_flag
FROM encounters_silver_id_seq_joined;


/* ---------------------------------------------------------
   Step 9 — Publish cleaned dataset (all years): silver_encounters_final
--------------------------------------------------------- */
CREATE OR REPLACE TEMP VIEW silver_encounters_final AS
SELECT
  CAST(encounter_id_clean        AS VARCHAR)  AS encounter_id,
  CAST(patient_id_txt            AS VARCHAR)  AS patient_id,
  CAST(encounter_date_parsed     AS DATE)     AS encounter_date,
  CAST(encounter_type_final      AS VARCHAR)  AS encounter_type,
  CAST(admission_date_parsed     AS DATE)     AS admission_date,
  CAST(discharge_date_parsed     AS DATE)     AS discharge_date,
  CAST(department_final          AS VARCHAR)  AS department,
  CAST(primary_diagnosis_final   AS VARCHAR)  AS primary_diagnosis,
  CAST(severity_level_final      AS VARCHAR)  AS severity_level,
  CAST(is_admitted_final         AS INTEGER)  AS is_admitted,
  CAST(length_of_stay_days_final AS INTEGER)  AS length_of_stay_days
FROM encounters_silver_encounter_id_synth;


/* ---------------------------------------------------------
   Step 10 — Publish KPI dataset (2025): gold_encounters_final
--------------------------------------------------------- */
CREATE OR REPLACE TEMP VIEW gold_encounters_final AS
SELECT
  encounter_id_clean        AS encounter_id,
  patient_id_txt            AS patient_id,
  encounter_date_parsed     AS encounter_date,
  encounter_type_final      AS encounter_type,
  admission_date_parsed     AS admission_date,
  discharge_date_parsed     AS discharge_date,
  department_final          AS department,
  primary_diagnosis_final   AS primary_diagnosis,
  severity_level_final      AS severity_level,
  is_admitted_final         AS is_admitted,
  length_of_stay_days_final AS length_of_stay_days,

  -- eligibility flags
  los_eligible_flag,
  readmission_eligible_flag,

  -- helper flags for KPI filtering
  CASE WHEN severity_level_final IN ('High','Critical') THEN 1 ELSE 0 END AS is_high_acuity,
  CASE WHEN encounter_type_final = 'Emergency' THEN 1 ELSE 0 END AS is_emergency
FROM encounters_silver_encounter_id_synth
WHERE encounter_date_parsed IS NOT NULL
  AND encounter_date_parsed >= DATE '2025-01-01'
  AND encounter_date_parsed <  DATE '2026-01-01';
