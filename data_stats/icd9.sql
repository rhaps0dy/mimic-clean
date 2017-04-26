SET search_path TO mimiciii;

DROP TABLE IF EXISTS diag_categories_icd9 CASCADE;
CREATE TABLE diag_categories_icd9 AS
WITH icd_ints AS
(SELECT
	row_id, subject_id, hadm_id, seq_num,
	(CASE WHEN icd9_code ILIKE 'E%' THEN 1000
		  WHEN icd9_code ILIKE 'V%' THEN 1001
		ELSE SUBSTRING(icd9_code FROM 0 FOR 4)::int END)
			AS icd_int
	FROM diagnoses_icd
)
SELECT
	row_id, subject_id, hadm_id, seq_num,
	(CASE WHEN icd_int < 140 THEN 0   --  ('001--139', 'Infections', 6736)
		  WHEN icd_int < 240 THEN 1   --  ('140--239', 'Neoplasms', 4144)
		  WHEN icd_int < 280 THEN 2   --  ('240--279', 'Endocrine, immunity', 15262)
		  WHEN icd_int < 290 THEN 3   --  ('280--289', 'Blood', 9381)
		  WHEN icd_int < 320 THEN 4   --  ('290--319', 'Mental', 12367)
		  WHEN icd_int < 360 THEN 5   --  ('320--359', 'Nervous system', 2098)
		  WHEN icd_int < 390 THEN 6   --  ('360--389', 'Sense organs', 16837)
		  WHEN icd_int < 460 THEN 7   --  ('390--459', 'Circulatory system', 10601)
		  WHEN icd_int < 520 THEN 8   --  ('460--519', 'Respiratory system', 9673)
		  WHEN icd_int < 580 THEN 9   --  ('520--579', 'Digestive system', 9717)
		  WHEN icd_int < 630 THEN 10  --   ('580--629', 'Genitourinary system', 63)
		  WHEN icd_int < 680 THEN 11  --  ('630--679', 'Pregnancy', 2887)
		  WHEN icd_int < 710 THEN 12  --  ('680--709', 'Skin', 5624)
		  WHEN icd_int < 740 THEN 13  --  ('710--739', 'Mussculoskeletal system', 919)
		  WHEN icd_int < 760 THEN 14  --  ('740--759', 'Congenital', 1)
		  WHEN icd_int < 780 THEN 15  --  ('760--779', 'Perinatal', 10261)
		  WHEN icd_int < 800 THEN 16  --  ('780--799', 'Symptoms, ill-defined', 10087)
		  WHEN icd_int < 1000 THEN 17 --  ('800--999', 'Injury, Poisoning', 9281)
		  WHEN icd_int = 1000 THEN 18 --  ('E codes', 'Accident', 12904)
		  WHEN icd_int = 1001 THEN 19
		ELSE -1 END) AS icd_cat
	FROM icd_ints WHERE icd_int IS NOT NULL;

-- https://en.wikipedia.org/wiki/List_of_ICD-9_codes



















