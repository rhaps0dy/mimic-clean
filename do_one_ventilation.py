#!/usr/bin/env python3

import psycopg2
import sys
import pickle
import re
import math
import csv
import itertools as it
from memoize_pickle import memoize_pickle
from extract_events import ex_float
from create_drug_durations import drugs as DRUGS
import collections

# b_vent_start, b_vent_end, pred_vent_will_return, pred_dies

table_headers = ['icustay_id', 'hour_first_vent', 'hour_vent', 'b_ventilator',
'pred_b_vent_again', 'pred_dies']

def do_one_icustay(cursor, icustay_id):
    cursor.execute("""SELECT info_icu_intime, info_dod, info_admit_time,
                   info_discharge_time FROM static_icustays
                   WHERE icustay_id=%s""", [icustay_id])
    in_time, dod, admit_time, discharge_time = cursor.fetchall()[0]
    cursor.execute("""SELECT starttime, endtime FROM ventdurations
                   WHERE icustay_id=%s""", [icustay_id])
    table_rows = collections.defaultdict(lambda: {}, {})
    first_endtime = None
    for starttime, endtime in cursor:
        if first_endtime is None:
            first_endtime = endtime
            def hour_index(time):
                return math.floor((time-first_endtime).total_seconds() / 3600)
        hour = hour_index(endtime)
        table_rows[hour] = {'b_exit_vent': 1, 'pred_b_vent_again': 1}
    if len(table_rows) == 0:
        return
    table_rows[max(table_rows.keys())]['pred_b_vent_again'] = 0
    table_headers = ['b_exit_vent', 'pred_b_vent_again']

    for d in DRUGS:
        cursor.execute("""SELECT starttime, endtime
                        FROM drugs.{:s}_durations
                        WHERE icustay_id={:d};""".format(d['drug_name'], icustay_id))
        for starttime, endtime in cursor:
            for hour in range(hour_index(starttime), hour_index(endtime)+1):
                table_rows[hour][d['drug_name']] = 1
        table_headers.append(d['drug_name'])

    

    # Fill rows:
    table_headers = ['icustay_id', 'hour'] + table_headers
    _min = min(table_rows.keys())
    _max = max(table_rows.keys())
    for i in range(_min, _max+1):
        table_rows[i]['icustay_id'] = icustay_id
        table_rows[i]['hour'] = i
        for t in table_headers:
                if t not in table_rows[i]:
                    table_rows[i][t] = 0

    items = list(table_rows.items())
    items.sort(key=lambda x: x[0])
    return table_headers, items

if __name__ == '__main__':
    conn_string = "host='localhost' dbname='adria' user='adria' password='adria'"
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("SET search_path TO mimiciii;")
    n_cursor = conn.cursor('n_cursor')

    table_headers, items = do_one_icustay(cursor, 223273)
    _, items_ = do_one_icustay(cursor, 200003)
    items += items_

    with open('one_ventilation.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=table_headers)
        writer.writeheader()
        for _, r in items:
            writer.writerow(r)
