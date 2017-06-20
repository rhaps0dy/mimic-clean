#!/usr/bin/env python3

from pkl_utils import *
import psycopg2
import itertools as it
from baseline import fetch_data

from create_48h_slices import get_frequent_headers, get_training_examples

@memoize_pickle('usable_hadm_ids.pkl.gz')
def usable_hadm_ids(cursor):
    cursor.execute("""SELECT hadm_id FROM static_icustays
        GROUP BY hadm_id HAVING COUNT(icustay_id NOT IN
            (SELECT icustay_id FROM chartevents GROUP BY icustay_id
                HAVING COUNT(itemid < 220000) = 0)
            ) = 0;""")
    return set(a[0] for a in cursor.fetchall())

@memoize_pickle('db_things.pkl.gz')
def fetch_db_things():
    conn_string = "host='localhost' dbname='adria' user='adria' password='adria'"
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("SET search_path TO mimiciii")
    hadm_ids = usable_hadm_ids(cursor)

    cursor.execute("""SELECT hadm_id, icustay_id, r_admit_time
        FROM static_icustays ORDER BY hadm_id, info_icu_intime""")
    icustay_drift_hadm = {}
    for hadm_id, icustay_id, r_admit_time in cursor:
        hour_diff = int(r_admit_time // 3600)
        icustay_drift_hadm[icustay_id] = (hadm_id, hour_diff)
    return hadm_ids, icustay_drift_hadm

@memoize_pickle('hadm_id_time_series.pkl.gz')
def hadm_id_time_series():
    hadm_ids, icustay_drift_hadm = fetch_db_things()

    df = get_frequent_headers()
    usable_keys = []
    for icustay_id, _ in df.iterrows():
        if icustay_id in icustay_drift_hadm:
            usable_keys.append(icustay_id)
    import code;
    code.interact(local=locals())
    usable_keys.sort()
    df = df.loc[usable_keys]


    for icustay_id, (_, hour_diff) in icustay_drift_hadm.items():
        try:
            if icustay_id in df.index:
                df[icustay_id, 'hour'] = df[icustay_id, 'hour'].map(lambda n: n+hour_diff)
        except KeyError:
            pass
    code.interact(local=locals())
    df.index = df.index.map(lambda i: icustay_drift_hadm[i][0])
    return icustay_drift_hadm, usable_keys, df

if __name__ == '__main__':
    icustay_drift_hadm, usable_keys, df = hadm_id_time_series()
    time_data = get_training_examples(df, 48)
    static_data = fetch_data("nobow", train_proportion=1.1)[0]
    static_data.index = static_data.index.map(lambda i: icustay_drift_hadm[i][0])
    static_data = static_data[['icustay_id', 'b_gender', 'r_age',
                               'i_previous_admissions', 'i_previous_icustays',]]


