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
import datetime
from extract_events import ex_float, METAVISION_MIN_ID
from do_one_ventilation import prepare_item_categories, get_item_names

def all_patient_icustays(cursor):
    """Get ICU stays for every patient, their starts and ends. Hours will be
    indexed with 0 at the input time, with negative times denoting hours
    previous to the particular stay."""
    cursor.execute("SELECT subject_id, icustay_id, info_icu_intime, info_icu_outtime, info_discharge_time FROM static_icustays WHERE r_age > 16")
    patients = collections.defaultdict(lambda: {}, {})
    for subject_id, icustay_id, intime, outtime, dischtime in cursor:
        patients[subject_id][icustay_id] = (intime, outtime or dischtime)

    no_icu = -1
    for s, d in patients.items():
        #icustays = list(d.items())
        #icustays.sort(key=lambda k: k[1][0])
        #for i, (id, (it, ot)) in enumerate(icustays):
        #    if ot is None:
        #        if i+1 < len(icustays):
        #            # No outtime before the last one should be None
        #            d[id] = (it, icustays[i+1][1][0])
        #            assert False
        #        else:
        #            # outtime should only be None when the patient dies
        #            cursor.execute("SELECT dod FROM patients WHERE subject_id=%s", [s])
        #            dod = cursor.fetchall()[0][0]
        #            assert dod is not None, "Patient {:d}".format(s)
        #            d[id] = (it, dod)
        # Add catch-all negative-id icustay after the last icustay
        last_outtime = d[max(d)][1]
        d[no_icu] = (last_outtime, None)
        no_icu -= 1
    return dict(patients)

class TableIter:
    def __init__(self, w_cursor, item_names, icustays):
        self.c = w_cursor
        self.item_names = item_names
        self.icustays = icustays
        self.headers = ['subject_id', 'icustay_id', 'hour']

    def __iter__(self):
        self.next_stop = False
        subject_id = None
        while subject_id not in self.icustays:
            subject_id, *_ = self.last_c = next(self.c)
        self.last_hour = None
        return self

    def hour_i_end(self, subject_id, icustay_id, time):
        "Returns the appropriate hour index and hour end time"
        if icustay_id is None:
            icustay_id = min(self.icustays[subject_id]) # The negative one
        intime, _ = self.icustays[subject_id][icustay_id]
        hour_index = math.floor((time-intime).total_seconds() / 3600)
        hour_end = intime + datetime.timedelta(hours=hour_index+1)
        return hour_index, hour_end, icustay_id

class chartevents(TableIter):
    def __init__(self, w_cursor, item_names, icustays, cursor):
        super(chartevents, self).__init__(w_cursor, item_names, icustays)
        it_cats = prepare_item_categories("chartevents")
        it_cats.sort(key=lambda e: -e[2]['frequency'])
        self.categories = {}
        for _, id, d in it_cats:
            if float in d['categories'] and (
                    d['categories'][float] >= d['frequency']*0.8 or
                    (len(d['categories']) == 2 and (
                        'Not available' in d['categories'] or
                        'Not applicable' in d['categories'])) or
                    id == 225953): #dextrose
                self.categories[id] = float
                self.headers.append('F ' + self.item_names[id])
            else:
                self.categories[id] = d['categories']
                self.headers.append('C ' + self.item_names[id])

        self.c.execute(("SELECT subject_id, icustay_id, itemid, charttime, "
                        "value, valuenum, valueuom "
                        "FROM chartevents "
                        "WHERE itemid >= %s "
                        "ORDER BY subject_id, icustay_id, charttime"),
                       [METAVISION_MIN_ID])

    def __next__(self):
        if self.next_stop:
            raise StopIteration
        subject_id, icustay_id, itemid, charttime, value, valuenum, valueuom = self.last_c
        # Skip all newborns
        while subject_id not in self.icustays:
            subject_id, icustay_id, itemid, charttime, value, valuenum, valueuom = self.last_c = next(self.c)

        hour_index, hour_end, icustay_id = self.hour_i_end(subject_id, icustay_id, charttime)
        initial_icustay_id = icustay_id
        r = {'subject_id': subject_id, 'icustay_id': icustay_id, 'hour': hour_index}
        # Not filling empty hours
        #if self.last_hour is not None and hour_index > self.last_hour + 1:
        #    self.last_hour += 1
        #    r['hour'] = self.last_hour
        #    return r
        #self.last_hour = hour_index
        divide_dict = collections.defaultdict(lambda: 0.0, {})
        value_dict = collections.defaultdict(lambda: 0.0, {})
        try:
            while charttime < hour_end and icustay_id == initial_icustay_id:
                cats = self.categories[itemid]
                if cats is float:
                    if valuenum is not None:
                        v = valuenum
                    else:
                        try:
                            match = ex_float.search(value)
                        except TypeError as e:
                            assert value is None, "That must be the reason why typeerror"
                            match = None
                        if match is None:
                            subject_id, icustay_id, itemid, charttime, value, \
                                valuenum, valueuom = self.last_c = next(self.c)
                            continue
                        v = float(match.group(1))
                    value_dict[itemid] += v
                    divide_dict[itemid] += 1.0
                else:
                    if ex_float.search(value) is not None:
                        print("itemid {:d}, value {:s} should not happen"
                              .format(itemid, value))
                    try:
                        value_dict[itemid] = cats[value]
                    except KeyError:
                        print("KeyError on {:d}, {:s}".format(itemid, value))
                subject_id, icustay_id, itemid, charttime, value, valuenum, \
                    valueuom = self.last_c = next(self.c)
            #if icustay_id != initial_icustay_id:
            #    self.last_hour = None
        except StopIteration:
            self.next_stop = True
        for i, v in value_dict.items():
            if i in divide_dict:
                v /= divide_dict[i]
            r[self.item_names[i]] = v
        return r

class outputevents(TableIter):
    def __init__(self, w_cursor, item_names, icustays, cursor):
        super(outputevents, self).__init__(w_cursor, item_names, icustays)
        print("doing main query...")
        self.c.execute(("SELECT subject_id, icustay_id, itemid, charttime, value "
                        "FROM outputevents "
                        "WHERE itemid >= %s "
                        "ORDER BY subject_id, icustay_id, charttime"),
                       [METAVISION_MIN_ID])
        print("Doing feature-count query")
        cursor.execute(("SELECT itemid, COUNT(row_id) "
                        "FROM outputevents WHERE itemid >= %s "
                        "GROUP BY itemid"),
                       [METAVISION_MIN_ID])
        hs = cursor.fetchall()
        hs.sort(key=lambda e: -e[1])
        hs = list(map(lambda e: 'F ' + item_names[e[0]], hs))
        self.headers += hs
        self.r_zeros = dict(zip(self.headers, it.repeat(0)))

    def __next__(self):
        if self.next_stop:
            raise StopIteration
        subject_id, icustay_id, itemid, charttime, value = self.last_c
        # Skip all newborns
        while subject_id not in self.icustays:
            subject_id, icustay_id, itemid, charttime, value = self.last_c = next(self.c)

        hour_index, hour_end, icustay_id = self.hour_i_end(subject_id, icustay_id, charttime)
        initial_icustay_id = icustay_id
        self.r_zeros['subject_id'] = subject_id
        self.r_zeros['icustay_id'] = icustay_id
        # We do want to write lost hour in outputevents, because they are 0
        if self.last_hour is not None and hour_index > self.last_hour + 1:
            self.last_hour += 1
            self.r_zeros['hour'] = self.last_hour
            return self.r_zeros
        self.last_hour = hour_index
        r = self.r_zeros.copy()
        r['hour'] = hour_index
        try:
            while charttime < hour_end and icustay_id == initial_icustay_id:
                r[self.item_names[itemid]] += value
                subject_id, icustay_id, itemid, charttime, value = \
                    self.last_c = next(self.c)
            if icustay_id != initial_icustay_id:
                self.last_hour = None
        except StopIteration:
            self.next_stop = True
        return r

def main():
    tables = {'chartevents', 'outputevents'}
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
    iterator = globals()[table](conn.cursor(table), item_names, icustays, cursor)
    with open(table+'.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=iterator.headers)
        writer.writeheader()
        i = 1
        print("Started iterating")
        for row in iterator:
            if i%100000 == 0:
                print("Doing row {:d}".format(i))
            writer.writerow(row)
            csvfile.flush()
            i += 1

if __name__ == '__main__':
    main()
