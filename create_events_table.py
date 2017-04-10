#!/usr/bin/env python3

####
# This file creates a table with hourly data for each patient
####

import psycopg2
import sys
import pickle
import re
import math
import csv
import itertools as it
import collections
from extract_events import ex_float, METAVISION_MIN_ID
from do_one_ventilation import prepare_item_categories, get_item_names

def all_patient_icustays(cursor):
    """Get ICU stays for every patient, their starts and ends. Hours will be
    indexed with 0 at the input time, with negative times denoting hours
    previous to the particular stay."""
    cursor.execute("SELECT subject_id, icustay_id, intime, outtime FROM icustays")
    patients = collections.defaultdict(lambda: {}, {})
    for subject_id, icustay_id, intime, outtime in cursor:
        patients[subject_id][icustay_id] = (intime, outtime)
    return dict(patients)

class SelfIter:
    def __iter__(self):
        return self

class chartevents(SelfIter):
    def __init__(self, w_cursor, item_names, icustays):
        self.c = w_cursor
        self.icustays = icustays
        it_cats = prepare_item_categories("chartevents")
        it_cats.sort(key=lambda e: -e[2]['frequency'])
        self.headers = []
        self.item_names = item_names
        self.categories = {}
        for _, id, d in it_cats:
            self.headers.append(self.item_names[id])
            if len(d) == 1 and float in d:
                self.categories[id] = float
            else:
                self.categories[id] = d

        self.c.execute(("SELECT subject_id, icustay_id, itemid, charttime, "
                        "value, valuenum, valueuom "
                        "FROM chartevents "
                        "WHERE itemid >= %s "
                        "ORDER BY subject_id, icustay_id, charttime"),
                       [METAVISION_MIN_ID])
        self.next_stop = False
        self.last_c = next(self.c)
        self.last_hour = None

    def __next__(self):
        if self.next_stop:
            raise StopIteration
        subject_id, icustay_id, itemid, charttime, value, valuenum, valueuom = self.last_c
        hour_index, hour_end = self.hour_i_end(subject_id, icustay_id, charttime)
        r = {'subject_id': subject_id, 'icustay_id': icustay_id, 'hour': hour_index}
        if self.last_hour is not None and hour_index > self.last_hour + 1:
            self.last_hour += 1
            r['hour'] = self.last_hour
            return r
        self.last_hour = hour_index
        divide_dict = collections.defaultdict(lambda: 0.0, {})
        value_dict = collections.defaultdict(lambda: 0.0, {})
        try:
            while charttime < hour_end:
                cats = self.categories[itemid]
                if cats is float:
                    if valuenum is not None:
                        v = valuenum
                    else:
                        match = ex_float.search(value)
                        if match is None:
                            continue
                        v = float(match.group(1))
                    value_dict[itemid] += v
                    divide_dict[itemid] += 1.0
                else:
                    if valuenum is not None:
                        print("Valuenum not none for itemid {:d} icustay_id {:d}"
                              .format(itemid, icustay_id))
                    value_dict[itemid] = cats[value]
                subject_id, icustay_id, itemid, charttime, value, valuenum, \
                    valueuom = self.last_c = next(self.c)
        except StopIteration:
            self.next_stop = True
        for i, d in divide_dict.items():
            value_dict[i] /= d
        for i, v in value_dict.items():
            if i in divide_dict:
                v /= divide_dict[i]
            r[self.item_names[i]] = v
        return r

    def hour_i_end(self, subject_id, icustay_id, time):
        "Returns the appropriate hour index and hour end time"
        intime, _ = self.icustays[subject_id][icustay_id]
        hour_index = math.floor((time-intime).total_seconds() / 3600)
        hour_end = intime + datetime.timedelta(hours=hour_index+1)
        return hour_index, hour_end




#datetimeevents = conn.cursor('datetimeevents')
#datetimeevents.execute(("SELECT subject_id, icustay_id, itemid, charttime, "
#                     "NULL AS value, NULL AS valuenum, NULL AS valueuom "
#                     "FROM datetimeevents "
#                     "WHERE itemid >= %s "
#                     "ORDER BY subject_id, icustay_id, charttime"),
#                    [METAVISION_MIN_ID])

def main():
    tables = {'chartevents'}
    if sys.argv[1] not in tables:
        print("Usage: {:s} {:s}".format(sys.argv[0], str(tables)))
        sys.exit(1)
    conn_string = "host='localhost' dbname='adria' user='adria' password='adria'"
    conn = psycopg2.connect(conn_string)
    table = sys.argv[1]
    cursor = conn.cursor()
    cursor.execute("SET search_path TO mimiciii")
    item_names = get_item_names(cursor)
    icustays = all_patient_icustays(cursor)
    iterator = globals()[table](conn.cursor(table), item_names, icustays)
    with open(table+'.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=iterator.headers)
        writer.writeheader()
        for row in iterator:
            writer.writerow(row)

if __name__ == '__main__':
    main()
