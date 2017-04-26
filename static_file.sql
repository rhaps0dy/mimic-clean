SET search_path TO mimiciii;

-- naming scheme: c_* for categorical variables, b_* for boolean variables, r_*
-- for real values, i_* for integers (the last 3 can be treated the same)

-- info_* is used for informational variables that we haven't converted to
-- numbers yet.
-- pred_* is used for possible prediction targets.

DROP TABLE IF EXISTS static_icustays CASCADE;
CREATE TABLE static_icustays AS
SELECT * FROM (
-- Subquery for filtering based on calculated expressions

SELECT
  i.hadm_id, i.icustay_id, i.subject_id,
  i.intime as info_icu_intime,
  i.outtime as info_icu_outtime,
  a.admittime as info_admit_time,
  EXTRACT(EPOCH FROM (a.admittime - i.intime)) AS r_admit_time,
  a.dischtime,
  -- Admission time relative to ICU entrance time.
  -- Doesn't take into account previous ICU stays in the same admission.

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
        WHEN a.ethnicity='MIDDLE EASTERN' THEN 7 -- (caucasian) total 44
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
-- WHERE s.r_admit_time < 0
-- -- This removes 486 patients, who are admitted after being put into the ICU. Of
-- -- those:
-- --  * More than 1h difference: 349
-- --  * More than 4h difference: 99
-- --  * More than a day difference: 2

WHERE s.c_admit_type >= 0
AND s.c_admit_location >= 0
AND s.c_insurance >= 0
AND s.c_marital_status >= 0
AND s.b_gender >= 0
AND s.c_ethnicity >= 0
-- All of this removes no patient
--) TO '/tmp/static_patients.csv' DELIMITER ',' CSV HEADER
;


-- Also, 98 people are admitted after being discharged. We have decided to
-- include them, and only check if the admission time is consistent with the
-- ICU intake time.

-- 44 patients have no ICU stay and are thus not reflected here
-- 46476 do have at least one. In total we have ~60k ICU stays

CREATE MATERIALIZED VIEW metavision_patients AS (SELECT DISTINCT(c.subject_id)
	FROM chartevents c JOIN static_icustays si ON c.subject_id=si.subject_id
	WHERE c.itemid >= 220000 AND si.r_age > 14);

-- Correct patients with several ethnicities
UPDATE static_icustays SET c_ethnicity=1 WHERE subject_id IN (1819, 2467, 3145,
	4784, 5215, 5242, 5710, 5897, 6063, 6090, 9206, 7698, 8559, 6214, 12110,
	6398, 6534, 9725, 13477, 12020, 10832, 10835, 15025, 13902, 16554, 90538,
	81926, 98347, 96100, 95561, 86279, 75401, 75393, 83210, 83156, 76803,
	71054, 69194, 81247, 71871, 58242, 59198, 62641, 50315, 63755, 51145,
	61711, 65431, 55337, 52736, 50140, 53804, 53739 15057, 16164, 16275, 16320,
	16516, 17728, 18756, 19104, 19241, 19296, 19470, 20236, 22122, 24402,
	24477, 24743, 24865, 25027, 25490, 25155, 25905, 26672, 27162, 27162,
	27879, 28600, 27083, 27337, 27464, 29121, 29142, 29999, 30129, 30155,
	30393, 30610, 31275, 31279, 32447, 32605, 40388, 42058, 42468, 42820,
	44715, 45985, 46057, 48693, 48752, 49649, 50721, 51628, 52125, 52307,
	54353, 56201, 57765, 58526, 59513, 59797, 64153, 68127, 72083, 76476,
	79556, 97659, 15641, 48340);
UPDATE static_icustays SET c_ethnicity=2 WHERE subject_id=(27374, 7241, 518,
	1018, 4390, 4392, 4900, 6510, 11242, 11285, 9261, 16685, 17798, 22851,
	22896, 29866, 26996, 46566, 62186, 68457, 65401, 55753, 70180, 77947,
	86276, 82202, 95765, 19338, 59301, 59301, 75658);

UPDATE static_icustays SET c_ethnicity=3 WHERE subject_id IN (13948, 16684,
	15679, 19493, 27210, 21318, 23100, 22505, 19827, 27106, 41902, 80015,
	92820, 7029, 8795, 18187, 20060, 30139, 17721);
UPDATE static_icustays SET c_ethnicity=4 WHERE subject_id IN (7533, 1104, 7533,
	11043, 15679);
UPDATE static_icustays SET c_ethnicity=5 WHERE subject_id IN (14667);
UPDATE static_icustays SET c_ethnicity=6 WHERE subject_id IN (62603, 59411,
	71244, 21955, 68605);
