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
from create_drug_durations import drugs as DRUGS

class TableIter:
    def __init__(self, w_cursor, cursor):
        self.c = w_cursor
        self.item_names = get_item_names(cursor)
        self.icustays = self.all_patient_icustays(cursor)
        # self.icustay_indices now contains the indices that have to be written
        self.icustay_completed = dict(zip(self.icustay_indices.keys(),
                                          it.repeat(False)))
        self.headers = ['subject_id', 'icustay_id', 'hour']

    def __iter__(self):
        self.next_stop = False
        self.last_hour = None
        self.empty_rows_subject_icustay = None
        subject_id = None
        while subject_id not in self.icustays:
            subject_id, icustay_id, *_ = self.last_c = next(self.c)
        return self

    def hour_i_end(self, subject_id, icustay_id, time):
        "Returns the appropriate hour index and hour end time"
        if subject_id not in self.icustays:
            # If we don't have a patient, it is because they are too young.
            # In this case they have no icustays
            return None, None, None
        if icustay_id is None:
            min_intime = None
            for icustay, (intime, outtime) in self.icustays[subject_id].items():
                if outtime >= time:
                    if min_intime is None or intime < min_intime:
                        min_intime = intime
                        icustay_id = icustay
            #icustay_id = min(self.icustays[subject_id]) # The negative one
            if icustay_id is None:
                print("Could not infer icustay_id")
                return None, None, None
        #    else:
        #        print("Inferred: subject_id", subject_id, "icustay_id", icustay_id, end=' ')
        #else:
        #    print("Got: subject_id", subject_id, "icustay_id", icustay_id, end=' ')
        intime, _ = self.icustays[subject_id][icustay_id]
        hour_index = math.floor((time-intime).total_seconds() / 3600)
        #print("Hour index", hour_index)
        hour_end = intime + datetime.timedelta(hours=hour_index+1)

        #if hour_index < self.icustay_indices[subject_id, icustay_id][0]:
        #    print("Hour index too early for", subject_id, icustay_id, hour_index, self.icustay_indices[subject_id, icustay_id][0])
        #elif hour_index > self.icustay_indices[subject_id, icustay_id][1]:
        #    print("Hour index too late for", subject_id, icustay_id, hour_index, self.icustay_indices[subject_id, icustay_id][1])

        return hour_index, hour_end, icustay_id

    def all_patient_icustays(self, cursor):
        """Get ICU stays for every patient, their starts and ends. Hours will be
        indexed with 0 at the input time, with negative times denoting hours
        previous to the particular stay."""
        cursor.execute("SELECT subject_id, icustay_id, info_icu_intime, info_icu_outtime, info_discharge_time FROM static_icustays WHERE r_age > 16")
        # Actually 9 patients are younger than 16; one of them is 14.9 years old
        patients = collections.defaultdict(lambda: {}, {})
        for subject_id, icustay_id, intime, outtime, dischtime in cursor:
            patients[subject_id][icustay_id] = (intime, outtime or dischtime)
        with open('icustay_indices.pkl', 'rb') as f:
            self.icustay_indices = icustay_indices = pickle.load(f)
        for subject_id, d in patients.items():
            for icustay_id, (intime, outtime) in d.items():
                if outtime is None:
                    d[icustay_id] = (intime, max(outtime, intime + datetime.timedelta(hours=icustay_indices[subject_id, icustay_id][1])))

            intervals = list(d.items())
            intervals.sort(key=lambda k: k[1][1])
            prev_outtime = None
            for icustay_id, (intime, outtime) in intervals:
                if prev_outtime is not None:
                    assert intime > prev_outtime, "Icustays intervals are disjoint"
                prev_outtime = outtime

        return dict(patients)

    def __next__(self):
        if self.next_stop:
            raise StopIteration
        if self.empty_rows_subject_icustay is not None:
            if self.last_hour < self.icustay_indices[self.empty_rows_subject_icustay][1]:
                self.last_hour += 1
                r = self.default_r.copy()
                r['subject_id'], r['icustay_id'] = self.empty_rows_subject_icustay
                r['hour'] = self.last_hour
                return r
            else:
                self.last_hour = None
                self.icustay_completed[self.empty_rows_subject_icustay] = True
                self.empty_rows_subject_icustay = None

        subject_id, icustay_id, charttime, *_ = self.last_c
        hour_index, hour_end, icustay_id = self.hour_i_end(subject_id, icustay_id, charttime)
        # Skip entries without an inferrable icustay
        while icustay_id is None:
            subject_id, icustay_id, charttime, *_ = self.last_c = next(self.c)
            hour_index, hour_end, icustay_id = \
                self.hour_i_end(subject_id, icustay_id, charttime)
        initial_subject_id = subject_id
        initial_icustay_id = icustay_id
        if self.icustay_completed[subject_id, icustay_id]:
            print("We actually completed ", icustay_id, self.last_c)
            sys.exit(1)

        # Fill empty hours
        r = self.default_r.copy()
        r['subject_id'] = subject_id
        r['icustay_id'] = icustay_id
        r['hour'] = hour_index
        if self.last_hour is None:
            self.last_hour = min(self.icustay_indices[subject_id, icustay_id][0],
                                 hour_index) - 1
        if hour_index > self.last_hour + 1:
            self.last_hour += 1
            r['hour'] = self.last_hour
            return r
        self.last_hour += 1
        assert self.last_hour == hour_index, \
            "last_hour {:d} != hour_index {:d}".format(self.last_hour, hour_index)

        self.prepare_last_c_processing()
        try:
            while charttime < hour_end and icustay_id == initial_icustay_id:
                self.process_last_c(r)

                icustay_id = None
                # Loop until we get a not-None icustay_id, checking subject_id
                # is not necessary, because if it changes then icustay_id will
                # also change, thus we will exit the outer loop
                while icustay_id is None:
                    subject_id, icustay_id, charttime, *_ = self.last_c = next(self.c)
                    hour_index, _, icustay_id = \
                        self.hour_i_end(subject_id, icustay_id, charttime)

            if icustay_id != initial_icustay_id:
                self.empty_rows_subject_icustay = (initial_subject_id, initial_icustay_id)
        except StopIteration:
            self.next_stop = True
        return self.return_last_c(r)


COLUMNS_TO_IGNORE = {227378 #patient location
}

class chartevents(TableIter):
    def __init__(self, w_cursor, cursor):
        super(chartevents, self).__init__(w_cursor, cursor)
        it_cats = prepare_item_categories("chartevents")
        it_cats.sort(key=lambda e: -e[2]['frequency'])
        self.categories = {}
        for _, id, d in it_cats:
            if id in COLUMNS_TO_IGNORE:
                continue

            if float in d['categories'] and (
                    d['categories'][float] >= d['frequency']*0.8 or
                    (len(d['categories']) == 2 and (
                        'Not available' in d['categories'] or
                        'Not applicable' in d['categories'])) or
                    id == 225953): #dextrose
                self.categories[id] = float
                self.item_names[id] = 'F ' + self.item_names[id]
            else:
                self.categories[id] = d['categories']
                self.item_names[id] = 'C ' + self.item_names[id]
            self.headers.append(self.item_names[id])

        self.c.execute(("SELECT subject_id, icustay_id, charttime, itemid, "
                        "value, valuenum, valueuom "
                        "FROM chartevents "
                        "WHERE itemid >= %s AND itemid NOT IN ({:s}) "
                        "ORDER BY subject_id, charttime, icustay_id"
                        .format(",".join(map(str, COLUMNS_TO_IGNORE)))),
                       [METAVISION_MIN_ID])
        self.default_r = {}

    def prepare_last_c_processing(self):
        self.divide_dict = collections.defaultdict(lambda: 0.0, {})
        self.value_dict = collections.defaultdict(lambda: 0.0, {})

    def process_last_c(self, _):
        # Unpack the last database row
        subject_id, icustay_id, charttime, itemid, value, valuenum, \
            valueuom = self.last_c
        #print("Processing", self.last_c)
        # Add the value to the divide and value dictionaries
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
                    return
                v = float(match.group(1))
            self.value_dict[itemid] += v
            self.divide_dict[itemid] += 1.0
        else:
            if ex_float.search(value) is not None:
                print("itemid {:d}, value {:s} should not happen"
                        .format(itemid, value))
            try:
                self.value_dict[itemid] = cats[value]
            except KeyError:
                print("KeyError on {:d}, {:s}".format(itemid, value))

    def return_last_c(self, r):
        for i, v in self.value_dict.items():
            if i in self.divide_dict:
                v /= self.divide_dict[i]
            r[self.item_names[i]] = v
        #print("Returning", r['subject_id'], r['icustay_id'], r['hour'])
        return r

class outputevents(TableIter):
    def __init__(self, w_cursor, cursor):
        super(outputevents, self).__init__(w_cursor, cursor)
        print("doing main query...")
        self.c.execute(("SELECT subject_id, icustay_id, charttime, itemid, value "
                        "FROM outputevents "
                        "WHERE itemid >= %s "
                        "ORDER BY subject_id, charttime, icustay_id"),
                       [METAVISION_MIN_ID])
        print("Doing feature-count query")
        cursor.execute(("SELECT itemid, COUNT(row_id) "
                        "FROM outputevents WHERE itemid >= %s "
                        "GROUP BY itemid"),
                       [METAVISION_MIN_ID])
        hs = cursor.fetchall()
        hs.sort(key=lambda e: -e[1])
        for id, _ in hs:
            self.item_names[id] = 'F ' + self.item_names[id]
            self.headers.append(self.item_names[id])
        self.default_r = dict(zip(self.headers, it.repeat(0)))

    def prepare_last_c_processing(self):
        pass

    def return_last_c(self, r):
        return r

    def process_last_c(self, r):
        _, _, _, itemid, value = self.last_c
        r[self.item_names[itemid]] += value

class drugevents(TableIter):
    def __init__(self, w_cursor, cursor):
        del w_cursor
        super(drugevents, self).__init__(None, cursor)

        clean_drugs = list(map(lambda s: 'B ' + s, DRUGS))
        self.headers += clean_drugs

        cursor.execute(("SELECT i.subject_id, d.icustay_id, d.starttime, "
                        "d.endtime "
                        "FROM drugs.{:s}_durations d JOIN icustays i "
                        "ON d.icustay_id=i.icustay_id "
                        "ORDER BY i.subject_id, d.icustay_id"))

    def __iter__(self):
        return iter(self.cleaned_list)

def main():
    tables = {'chartevents', 'outputevents', 'drugevents'}
    if sys.argv[1] not in tables:
        print("Usage: {:s} {:s}".format(sys.argv[0], str(tables)))
        sys.exit(1)
    conn_string = "host='localhost' dbname='adria' user='adria' password='adria'"
    conn = psycopg2.connect(conn_string)
    table = sys.argv[1]
    cursor = conn.cursor()
    cursor.execute("SET search_path TO mimiciii")
    iterator = globals()[table](conn.cursor(table), cursor)
    with open(table+'.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=iterator.headers)
        writer.writeheader()
        i = 1
        print("Started iterating")
        prev_row = {'subject_id': 0, 'icustay_id': 0, 'hour': 0}
        for row in iterator:
            if i%100000 == 0:
                print("Doing row {:d}".format(i))
            writer.writerow(row)
            csvfile.flush()

            assert row['subject_id'] >= 0
            assert row['icustay_id'] >= 0
            if row['icustay_id'] == prev_row['icustay_id']:
                assert row['hour'] == prev_row['hour'] + 1, \
                    "hour {:d} != prev_hour {:d} + 1".format(row['hour'], prev_row['hour'])
            prev_row = row
            i += 1

if __name__ == '__main__':
    main()
