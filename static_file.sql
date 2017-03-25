SET search_path TO mimiciii;

-- naming scheme: c_* for categorical variables, b_* for boolean variables, r_*
-- for real values, i_* for integers (the last 3 can be treated the same)

-- info_* is used for informational variables that we haven't converted to
-- numbers yet.
-- pred_* is used for possible prediction targets.

DROP MATERIALIZED VIEW IF EXISTS static_icustays;
CREATE MATERIALIZED VIEW static_icustays as
SELECT * FROM (
-- Subquery for filtering based on calculated expressions

SELECT
  i.icustay_id,
  i.intime as info_icu_intime,
  a.admittime as info_admit_time,
  EXTRACT(EPOCH FROM (a.admittime - i.intime)) AS r_admit_time,
  a.dischtime,
  -- Admission time relative to ICU entrance time.
  -- Doesn't take into account previous ICU stays in the same admission.

  i.hadm_id, i.icustay_id, i.subject_id,

  (CASE WHEN a.admission_type='ELECTIVE' THEN 0
        WHEN a.admission_type='NEWBORN' THEN 1
        WHEN a.admission_type='URGENT' THEN 3
        WHEN a.admission_type='EMERGENCY' THEN 4
        ELSE -1 END) AS c_admit_type,

  (CASE WHEN a.admission_location='** INFO NOT AVAILABLE **' THEN 0
        WHEN a.admission_location='EMERGENCY ROOM ADMIT' THEN 1
        WHEN a.admission_location='TRANSFER FROM HOSP/EXTRAM' THEN 2
        WHEN a.admission_location='TRANSFER FROM OTHER HEALT' THEN 3
        WHEN a.admission_location='CLINIC REFERRAL/PREMATURE' THEN 4
        WHEN a.admission_location='TRANSFER FROM SKILLED NUR' THEN 5
        WHEN a.admission_location='TRSF WITHIN THIS FACILITY' THEN 6
        WHEN a.admission_location='HMO REFERRAL/SICK' THEN 7
        WHEN a.admission_location='PHYS REFERRAL/NORMAL DELI' THEN 8
        ELSE -1 END) AS c_admit_location,

  (CASE WHEN a.insurance='Medicare' THEN 0
        WHEN a.insurance='Medicaid' THEN 1
        WHEN a.insurance='Government' THEN 2
        WHEN a.insurance='Private' THEN 3
        WHEN a.insurance='Self Pay' THEN 4
        ELSE -1 END) AS c_insurance,

  (CASE WHEN a.marital_status IS NULL
          OR a.marital_status='UNKNOWN (DEFAULT)' THEN 0
        WHEN a.marital_status='SINGLE' THEN 1
        WHEN a.marital_status='MARRIED' THEN 2
        WHEN a.marital_status='SEPARATED' THEN 3
        WHEN a.marital_status='DIVORCED' THEN 4
        WHEN a.marital_status='LIFE PARTNER' THEN 5
        WHEN a.marital_status='WIDOWED' THEN 6
        ELSE -1 END) AS c_marital_status,

  (CASE WHEN a.ethnicity IS NULL
          OR a.ethnicity='UNKNOWN/NOT SPECIFIED'
          OR a.ethnicity='UNABLE TO OBTAIN'
          OR a.ethnicity='PATIENT DECLINED TO ANSWER' THEN 0 -- total 6085
        -- Numbers ordered by decreasing population after this
        WHEN a.ethnicity LIKE '%BRAZIL%' -- some say 'WHITE/BRAZIL', others 'BRAZIL'
                                         -- that is why it is up here
          OR a.ethnicity='PORTUGUESE' THEN 3                 -- total 132
        WHEN a.ethnicity LIKE 'WHITE%' THEN 1                -- total 42573
        WHEN a.ethnicity LIKE 'BLACK%' THEN 2                -- total 5963
        WHEN a.ethnicity LIKE 'HISPANIC%' THEN 3             -- total 2167
        WHEN a.ethnicity LIKE 'ASIAN%' THEN 4                -- total 2015
        WHEN a.ethnicity LIKE 'AMERICAN INDIAN%' THEN 5      -- total 56
        WHEN a.ethnicity='MIDDLE EASTERN' THEN 1 -- (caucasian) total 44
        WHEN a.ethnicity LIKE 'NATIVE HAWAIIAN%' THEN 5      -- total 18
        WHEN a.ethnicity='CARIBBEAN ISLAND' THEN 5           -- total 9
        WHEN a.ethnicity='SOUTH AMERICAN' THEN 5             -- total 9
        WHEN a.ethnicity='OTHER'
          OR a.ethnicity LIKE 'MULTI%' THEN 6 -- total 1679
        ELSE -1 END) AS c_ethnicity,

  (CASE WHEN p.gender='M' THEN 0
        WHEN p.gender='F' THEN 1
        ELSE -1 END) AS b_gender,

  -- Some patients have age 300, which is actually 90
  LEAST(EXTRACT(EPOCH FROM (i.intime - p.dob)) / (3600 * 24 * 365), 90) as r_age,

  (SELECT COUNT(*) FROM admissions t
   WHERE t.subject_id = i.subject_id
   AND t.admittime < a.admittime) AS i_previous_admissions,

  (SELECT COUNT(*) FROM icustays t
   WHERE t.subject_id = i.subject_id
   AND t.intime < i.intime) AS i_previous_icustays,

  a.dischtime as info_discharge_time,
  EXTRACT(EPOCH FROM (a.dischtime - i.intime)) as r_pred_discharge_time,
  (CASE WHEN a.discharge_location LIKE 'HOME' THEN 0
        WHEN a.discharge_location LIKE '%HOME%' THEN 1 -- includes HOME HEALTH
                                                       -- CARE, HOSPICE-HOME
        WHEN a.discharge_location LIKE 'DEAD%' THEN 2
        ELSE 3 END) as c_pred_discharge_location,
        -- bunch of things that sound like somewhat serious conditions

  p.dod as info_dod,
  EXTRACT(EPOCH FROM (p.dod - i.intime)) as r_pred_death_time,
  -- Some death times are inconsistent between a.deathtime, a.dischtime and p.dod.
  -- patients.dod is used as the authoritative source, since it is strictly more
  -- complete. (no dead patient has a NULL date of death there)
  -- Total of 5790 ICU-stays of patients with discrepancies

  (CASE WHEN p.dod_hosp IS NULL THEN 0 ELSE 1 END) as b_pred_died_in_hospital,

  a.diagnosis as INFO_diagnosis,
  a.language as INFO_language,
  a.religion as INFO_religion

FROM icustays i LEFT JOIN patients p ON (i.subject_id = p.subject_id)
LEFT JOIN admissions a ON (i.hadm_id = a.hadm_id)
) s
WHERE s.r_admit_time < 0
-- This removes 486 patients, who are admitted after being put into the ICU. Of
-- those:
--  * More than 1h difference: 349
--  * More than 4h difference: 99
--  * More than a day difference: 2

AND s.c_admit_type >= 0
AND s.c_admit_location >= 0
AND s.c_insurance >= 0
AND s.c_marital_status >= 0
AND s.b_gender >= 0
AND s.c_ethnicity >= 0
-- All of this removes no patient
) TO '/tmp/static_patients.csv' DELIMITER ',' CSV HEADER
;


-- Also, 98 people are admitted after being discharged. We have decided to
-- include them, and only check if the admission time is consistent with the
-- ICU intake time.

-- 44 patients have no ICU stay and are thus not reflected here
-- 46476 do have at least one. In total we have ~60k ICU stays
