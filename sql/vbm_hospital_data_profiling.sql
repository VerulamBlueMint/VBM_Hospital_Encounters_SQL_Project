
/*
============================================================
  File: vbm_hospital_data_profiling.sql
  Project: Verulam Blue Mint — Healthcare Encounters (C08_l01)
  Dataset: C08_l01_healthcare_encounters_data_table
  Author: Verulam Blue

  Description:
    Data Profiling: Diagnostics, used only for EDA
============================================================
*/

/* ---------------------------------------------------------
   Profiling (no transformations)
--------------------------------------------------------- */

-- Quick peek (sample only)
SELECT *
FROM C08_l01_healthcare_encounters_data_table
LIMIT 20;

-- Row counts + distinct IDs
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT encounter_id) AS distinct_encounter_ids,
  COUNT(DISTINCT patient_id)   AS distinct_patient_ids
FROM C08_l01_healthcare_encounters_data_table;

-- Null/blank baseline for key fields (treat blanks as missing)
SELECT
  SUM(CASE WHEN encounter_id         IS NULL THEN 1 ELSE 0 END) AS null_encounter_id,
  SUM(CASE WHEN encounter_date       IS NULL OR TRIM(encounter_date)       = '' THEN 1 ELSE 0 END) AS null_encounter_date,
  SUM(CASE WHEN admission_date       IS NULL OR TRIM(admission_date)       = '' THEN 1 ELSE 0 END) AS null_admission_date,
  SUM(CASE WHEN discharge_date       IS NULL OR TRIM(discharge_date)       = '' THEN 1 ELSE 0 END) AS null_discharge_date,
  SUM(CASE WHEN department           IS NULL OR TRIM(department)           = '' THEN 1 ELSE 0 END) AS null_department,
  SUM(CASE WHEN severity_level       IS NULL OR TRIM(severity_level)       = '' THEN 1 ELSE 0 END) AS null_severity_level,
  SUM(CASE WHEN primary_diagnosis    IS NULL OR TRIM(primary_diagnosis)    = '' THEN 1 ELSE 0 END) AS null_primary_diagnosis,
  SUM(CASE WHEN length_of_stay_days  IS NULL THEN 1 ELSE 0 END) AS null_length_of_stay_days,
  SUM(CASE WHEN is_admitted          IS NULL THEN 1 ELSE 0 END) AS null_is_admitted
FROM C08_l01_healthcare_encounters_data_table;


