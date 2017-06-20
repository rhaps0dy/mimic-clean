#!/usr/bin/env python3

####
# This file is used to check whether all ventilation periods fall inside of an
# inferred icustay, as contained in the `icustay_indices.pkl` file.
####

import psycopg2
import sys
import pickle
import re
import math
import csv
import itertools as it
import collections
import datetime
from extract_events import ex_float, METAVISION_MIN_ID
from create_events_table import TableIter
import pickle

if __name__ == '__main__':
    with open('icustay_indices.pkl', 'rb') as f:
        icustay_indices = pickle.load(f)


    conn_string = "host='localhost' dbname='adria' user='adria' password='adria'"
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("SET search_path TO mimiciii")
    ti = TableIter(None, cursor)
    cursor.execute("""SELECT i.subject_id, v.icustay_id, v.starttime, v.endtime
                      FROM ventdurations v JOIN icustays i
                      ON v.icustay_id = i.icustay_id""")
    for subject_id, icustay_id, starttime, endtime in cursor:
        if subject_id in ti.icustays:
            if icustay_id in ti.icustays[subject_id]:
                s_hi, _, _ = ti.hour_i_end(subject_id, icustay_id, starttime)
                e_hi, _, _ = ti.hour_i_end(subject_id, icustay_id, endtime)
                if (subject_id, icustay_id) in icustay_indices:
                    if not icustay_indices[subject_id, icustay_id][0] <= s_hi:
                        print("bad start", icustay_id, icustay_indices[subject_id, icustay_id][0], s_hi)
                        icustay_indices[subject_id, icustay_id] = (s_hi, icustay_indices[subject_id, icustay_id][1])
                    if not icustay_indices[subject_id, icustay_id][1] >= e_hi:
                        print("bad end", icustay_id, icustay_indices[subject_id, icustay_id][1], e_hi)
                        icustay_indices[subject_id, icustay_id] = (icustay_indices[subject_id, icustay_id][0], e_hi)
