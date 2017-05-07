#!/USSR/bin/env python3

####
# This file extracts all of one patient ventilation event's time data
####

import psycopg2
import sys
import pickle
import re
import math
import csv
import itertools as it
from extract_events import ex_float, METAVISION_MIN_ID
from create_drug_durations import drugs as DRUGS
import collections

def get_item_names(cursor):
    cursor.execute("SELECT itemid, label FROM d_items")
    return dict(iter(cursor))

def prepare_item_categories(table, only_metavision=True, with_table=True):
    with open('{:s}_item_categories.pkl'.format(table), 'rb') as f:
        item_categories = pickle.load(f)
    item_categories = list(item_categories.items())
    if only_metavision:
        item_categories = list(filter(lambda a: a[0] >= METAVISION_MIN_ID, item_categories))
    if with_table:
        return list(map(lambda t: (table,) + t, item_categories))
    return item_categories

def get_item_categories(cursor):
    chartevents_item_categories = prepare_item_categories('chartevents')
    #labevents_item_categories = prepare_item_categories('labevents')

    #item_categories = chartevents_item_categories + labevents_item_categories
    item_categories = chartevents_item_categories
    item_categories.sort(key=lambda i: -i[2]['frequency'])
    return item_categories[:500]

class ProcessTuple:
    tuples = {
        "chartevents": "value, valuenum, valueuom, charttime",
    }
    order = {
        "chartevents": "charttime",
    }
    @staticmethod
    def chartevents(cat_freq, itemid, value, valuenum, valueuom, charttime):
        categories = cat_freq['categories']
        if itemid == 113 and valueuom == '%':
            # Skip the ones that are not mmHg
            item_type = 'skip'
        elif float in categories and categories[float] >= cat_freq['frequency']*0.8:
            item_type = 'float'
            if valuenum is not None:
                item_value = valuenum
            else:
                match = ex_float.search(value)
                if match is not None:
                    item_value = float(match.group(1))
                else:
                    item_type = 'skip'
                    item_value = None
        else:
            if value is not None:
                item_type = 'categorical'
                item_value = categories[value]
            else:
                item_type = 'skip'
                item_value = None
        return item_type, item_value, charttime

def get_events(cursor, hour_index, out_rows, table, itemid, label, cat_freq, person_id_name, person_id):
    cursor.execute(("SELECT {:s} FROM {:s} WHERE {:s}=%s AND itemid=%s ORDER BY {:s}"
                   .format(ProcessTuple.tuples[table],
                           table,
                           person_id_name,
                           ProcessTuple.order[table])),
                   [person_id, itemid])
    cur_hour = None
    cur_n = 0
    cur_sum = 0
    for row in cursor:
        item_type, item_value, charttime = getattr(ProcessTuple, table)(cat_freq, itemid, *row)
        hour = hour_index(charttime)
        if item_type == 'skip':
            continue
        elif item_type == 'categorical':
            out_rows[hour][label] = item_value
            continue

        if hour != cur_hour:
            if cur_hour is not None:
                out_rows[cur_hour][label] = cur_sum / cur_n
            cur_hour = hour
            cur_n = 0
            cur_sum = 0
        cur_n += 1
        cur_sum += item_value
    if cur_hour is not None:
        out_rows[cur_hour][label] = cur_sum / cur_n


def do_one_icustay(cursor, item_names, item_categories, subject_id, icustay_id):
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
        table_rows[hour]['b_exit_vent'] = 1
        table_rows[hour]['pred_b_vent_again'] = 1
        table_rows[hour_index(starttime)]['b_enter_vent'] = 1
    if len(table_rows) == 0:
        # That is, if there was no ventilation.
        return None, []
    table_rows[max(table_rows.keys())]['pred_b_vent_again'] = 0
    table_headers = ['b_exit_vent', 'b_enter_vent', 'pred_b_vent_again']

    for d in DRUGS:
        cursor.execute("""SELECT starttime, endtime
                        FROM drugs.{:s}_durations
                        WHERE icustay_id={:d};""".format(d['drug_name'], icustay_id))
        for starttime, endtime in cursor:
            for hour in range(hour_index(starttime), hour_index(endtime)+1):
                table_rows[hour][d['drug_name']] = 1
        table_headers.append(d['drug_name'])

    # All other headers will have zeroes filled in where nulls are
    null_allowed_headers = set()
    for table, itemid, cat_freq in item_categories:
        label = item_names[itemid]
        print("Doing item {:s}".format(label))
        if table == 'labevents':
            get_events(cursor, hour_index, table_rows, table, itemid, label,
                cat_freq, 'subject_id', subject_id)
        else:
            get_events(cursor, hour_index, table_rows, table, itemid, label,
                cat_freq, 'icustay_id', icustay_id)
        null_allowed_headers.add(label)
        table_headers.append(label)

    # Fill rows:
    table_headers = ['icustay_id', 'hour'] + table_headers
    _min = min(table_rows.keys())
    _max = max(table_rows.keys())
    for i in range(_min, _max+1):
        table_rows[i]['icustay_id'] = icustay_id
        table_rows[i]['hour'] = i
        for t in table_headers:
            if t not in table_rows[i] and t not in null_allowed_headers:
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

    # Get the known items and the possible values they can take
    item_names = get_item_names(cursor)
    item_categories = get_item_categories(cursor)

    cursor.execute("""SELECT DISTINCT(si.subject_id, si.icustay_id)
                   FROM static_icustays si JOIN chartevents c ON si.icustay_id=c.icustay_id
                   WHERE r_age > 15 AND c.itemid >= %s""", [METAVISION_MIN_ID])
    patients = cursor.fetchall()
    prev_table_headers = None
    # Go through all patients and write their items to the file
    for subject_id, icustay_id in map(lambda p: eval(p[0]), patients):
        print("Doing patient {:d}, {:d}".format(subject_id, icustay_id))
        table_headers, items = do_one_icustay(cursor, item_names, item_categories, subject_id, icustay_id)
        if len(items) == 0:
            print("No ventilations for patient {:d}, {:d}".format(subject_id, icustay_id))
            continue
        if prev_table_headers is None:
            csvfile = open('one_ventilation.csv', 'w')
            writer = csv.DictWriter(csvfile, fieldnames=table_headers)
            writer.writeheader()
        else:
            assert prev_table_headers == table_headers, "Some patient returned different headers"
        prev_table_headers = table_headers
        for _, r in items:
            writer.writerow(r)
    csvfile.close()
