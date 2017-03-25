#!/usr/bin/env python3

import psycopg2

drugs = [{
    "drug_name": "dobutamine",
    "cv_itemid_test": "in (30042,30306)",
    "mv_itemid_test": "= 221653",
}, {
    "drug_name": "adenosine",
    "cv_itemid_test": "= 4649",
    "mv_itemid_test": "= 221282",
    "cv_vaso_null": "valuenum",
    "cv_vaso_rate": "valuenum",
    "cv_vaso_amount": "valuenum",
    "cv_table": "chartevents",
}, {
    "drug_name": "dopamine",
    "cv_itemid_test": "in (30043,30307)",
    "mv_itemid_test": "= 221662",
}, {
    "drug_name": "epinephrine",
    "cv_itemid_test": "in (30044,30119,30309)",
    "mv_itemid_test": "= 221289",
}, {
    "drug_name": "isuprel",
    "cv_itemid_test": "= 30046",
    "mv_itemid_test": "= 227692",
}, {
    "drug_name": "milrinone",
    "cv_itemid_test": "= 30125",
    "mv_itemid_test": "= 221986",
}, {
    "drug_name": "norepinephrine",
    "cv_itemid_test": "in (30047,30120)",
    "mv_itemid_test": "= 221906",
}, {
    "drug_name": "phenylephrine",
    "cv_itemid_test": "in (30127,30128)",
    "mv_itemid_test": "= 221749",
}, {
    "drug_name": "vasopressin",
    "cv_itemid_test": "= 30051",
    "mv_itemid_test": "= 222315",
}, {
    "drug_name": "vancomycin",
    "cv_itemid_test": "= 3679",
    "mv_itemid_test": "= 225798",
    "cv_table": "chartevents",
    "cv_vaso_null": "value",
    "cv_vaso_rate": "(case when value is null then 0 else 1 end)",
    "cv_vaso_amount": "(case when value is null then 0 else 1 end)",
}, {
    "drug_name": "piperacillin",
    "cv_itemid_test": "= 5380",
    "mv_itemid_test": "in (225892,225893)",
    "cv_table": "chartevents",
    "cv_vaso_null": "value",
    "cv_vaso_rate": "(case when value is null then 0 else 1 end)",
    "cv_vaso_amount": "(case when value is null then 0 else 1 end)",
}, {
    "drug_name": "meropenem",
    "cv_itemid_test": "in (4587, 5060, 5062)",
    "mv_itemid_test": "= 225883",
    "cv_table": "chartevents",
    "cv_vaso_null": "value",
    "cv_vaso_rate": "(case when value is null then 0 else 1 end)",
    "cv_vaso_amount": "(case when value is null then 0 else 1 end)",
}, {
    "drug_name": "metronidazole",
    "cv_itemid_test": "= -1",
    "mv_itemid_test": "= 225884",
}, {
    "drug_name": "cefotaxime",
    "cv_itemid_test": "in (4215, 4256, 4364)",
    "mv_itemid_test": "= -1",
    "cv_table": "chartevents",
    "cv_vaso_null": "value",
    "cv_vaso_rate": "(case when value is null then 0 else 1 end)",
    "cv_vaso_amount": "(case when value is null then 0 else 1 end)",
}, {
    "drug_name": "cefepime",
    "cv_itemid_test": "= -1",
    "mv_itemid_test": "= 225851",
}, {
    "drug_name": "ciprofloxacin",
    "cv_itemid_test": "= -1",
    "mv_itemid_test": "= 225859",
}, {
    "drug_name": "levofloxacin",
    "cv_itemid_test": "= -1",
    "mv_itemid_test": "= 225879",
}, {
    "drug_name": "gentamicin",
    "cv_itemid_test": "in (45101, 45119, 45294, 45644, 45647)",
    "mv_itemid_test": "= 225875",
# Only one person ever is on prismasate so we can ignore it
#}, {
#    "drug_name": "prismasate",
#    "cv_itemid_test": "= 46693",
#    "mv_itemid_test": "in (225162, 225163)",
}, {
    "drug_name": "furosemide",
    "cv_itemid_test": "= 30123", # in (4219, 3439, 4888, 6120, 7780) in chartevents
    "mv_itemid_test": "in (228340, 221794)",
}, {
    "drug_name": "sodium_bicarbonate",
    "cv_itemid_test": "in (30030, 44166, 46592, 46362)", # 46362 bicarbonate, 44166 bicarbonate-hco3
    "mv_itemid_test": "in (221211, 220995, 227533, 225165)",
}, {
    "drug_name": "albumin",
    "cv_itemid_test": "in (40548, 30181, 30008, 30009, 44952, 42832, 46564, 44203, 43237, 43353, 45403)",
    "mv_itemid_test": "in (220861, 220862, 220863, 220864)",
}]

for d in drugs:
    if "cv_table" not in d:
        d["cv_table"] = "inputevents_cv"
        d["cv_vaso_null"] = "rate"
        d["cv_vaso_rate"] = "rate"
        d["cv_vaso_amount"] = "amount"

if __name__ == '__main__':
    conn_string = "host='localhost' dbname='adria' user='adria' password='adria'"
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    with open('generic_drug_durations.sql', 'r') as f:
        drug_sql = f.read()
    for d in drugs:
        with open('DRUG_{drug_name:s}.sql'.format(**d), 'w') as f:
            f.write(drug_sql.format(**d))
