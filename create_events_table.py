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
from item_categories import ex_float
from create_drug_durations import drugs as DRUGS
import pickle_utils as pu

METAVISION_MIN_ID = 220000

class HourMixin:
    """ Set the tables to be created by hours.

    It is possible to change the time period by changing
    TableIter.period_length, possibly by changing this mixin. """

    period_length = 3600.

class TableIter:
    def __init__(self, w_cursor, cursor):
        self.c = w_cursor
        # item_names: the label of a given item_id
        self.item_names = pu.load('item_names.pkl.gz')

        # icustays: a dictionary of d[subject_id][icustay_id] = (in_time, out_time)
        # icustay_indices: a set of (subject_id, icustay_id)
        self.icustays, self.icustay_indices = self.all_patient_icustays(cursor)

        self.icustay_completed = dict(zip(
            self.icustay_indices, it.repeat(False)))

        self.headers = ['subject_id', 'icustay_id', 'hour']

    def __iter__(self):
        self.next_stop = False
        subject_id = None
        try:
            # Iterate until the first relevant entry in the table
            while subject_id not in self.icustays:
                subject_id, icustay_id, *_ = self.last_c = next(self.c)
        except StopIteration:
            self.next_stop = True
        return self

    def hour_i_end(self, subject_id, icustay_id, time):
        "Returns the appropriate hour index and hour end time"
        if subject_id not in self.icustays:
            # If we don't have a patient, it is because they are not selected.
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
                #print("Could not infer icustay_id")
                return None, None, None
        #    else:
        #        print("Inferred: subject_id", subject_id, "icustay_id", icustay_id, end=' ')
        #else:
        #    print("Got: subject_id", subject_id, "icustay_id", icustay_id, end=' ')
        intime, _ = self.icustays[subject_id][icustay_id]
        hour_index = math.floor((time-intime).total_seconds() / self.period_length)
        #print("Hour index", hour_index)
        hour_end = intime + datetime.timedelta(seconds=(hour_index+1)*self.period_length)

        return hour_index, hour_end, icustay_id

    def all_patient_icustays(self, cursor):
        """Get ICU stays for every patient, their starts and ends. Hours will be
        indexed with 0 at the input time, with negative times denoting hours
        previous to the particular stay."""
        cursor.execute("SELECT subject_id, icustay_id, info_icu_intime, "
                       "info_icu_outtime, info_discharge_time "
                       "FROM selected_patients")
        patients = collections.defaultdict(lambda: {}, {})
        for subject_id, icustay_id, intime, outtime, dischtime in cursor:
            patients[subject_id][icustay_id] = (intime, outtime or dischtime)

        sid_iid = set()
        for subject_id, d in patients.items():
            for icustay_id, (intime, outtime) in d.items():
                assert outtime is not None
                sid_iid.add((subject_id, icustay_id))

            intervals = list(d.items())
            intervals.sort(key=lambda k: k[1][1])
            prev_outtime = None
            for icustay_id, (intime, outtime) in intervals:
                if prev_outtime is not None:
                    assert intime > prev_outtime, "Icustays intervals are disjoint"
                prev_outtime = outtime

        return dict(patients), sid_iid

    def __next__(self):
        if self.next_stop:
            raise StopIteration

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
                self.icustay_completed[initial_subject_id, initial_icustay_id] = True
        except StopIteration:
            self.next_stop = True
        return self.return_last_c(r)


COLUMNS_TO_IGNORE = {227378 #patient location
}

class chartevents(TableIter, HourMixin):
    def __init__(self, w_cursor, cursor):
        super(chartevents, self).__init__(w_cursor, cursor)
        self.translation = pu.load("translation_chartevents.pkl.gz")
        it_cats = list(pu.load("chartevents_item_categories.pkl.gz").items())
        it_cats.sort(key=lambda e: -e[1]['frequency'])
        self.categories = {}
        for id, d in it_cats:
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
                        "AND hadm_id IN (SELECT hadm_id FROM selected_patients) "
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
        if itemid in self.translation:
            itemid = self.translation[itemid]
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
                gs = match.groups()
                v = float(gs[0] or gs[1])
            self.value_dict[itemid] += v
            self.divide_dict[itemid] += 1.0
        else:
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

class outputevents(TableIter, HourMixin):
    def __init__(self, w_cursor, cursor):
        super(outputevents, self).__init__(w_cursor, cursor)
        print("doing main query...")
        self.c.execute(("SELECT subject_id, icustay_id, charttime, itemid, value "
                        "FROM outputevents "
                        "WHERE itemid >= %s "
                        " AND icustay_id IN (SELECT icustay_id FROM selected_patients) "
                        "ORDER BY subject_id, charttime, icustay_id"),
                       [METAVISION_MIN_ID])
        print("Doing feature-count query")
        cursor.execute(("SELECT itemid, COUNT(row_id) "
                        "FROM outputevents WHERE itemid >= %s "
                        " AND icustay_id IN (SELECT icustay_id FROM selected_patients) "
                        "GROUP BY itemid"),
                       [METAVISION_MIN_ID])
        hs = cursor.fetchall()
        hs.sort(key=lambda e: -e[1])
        for id, _ in hs:
            self.item_names[id] = 'F ' + self.item_names[id]
            self.headers.append(self.item_names[id])
        self.default_r = collections.defaultdict(lambda: 0.0, {})

    def prepare_last_c_processing(self):
        pass

    def return_last_c(self, r):
        return r

    def process_last_c(self, r):
        _, _, _, itemid, value = self.last_c
        r[self.item_names[itemid]] += value

class drugevents(TableIter, HourMixin):
    default_fill = None
    def __init__(self, w_cursor, cursor):
        del w_cursor
        super(drugevents, self).__init__(None, cursor)
        self.event_intervals = collections.defaultdict(
            lambda: collections.defaultdict(lambda: [], {}),
            {})

        self.icustay_indices = dict(zip(self.icustay_indices,
                                        it.repeat((math.inf, -math.inf))))
        self.prepare_event_intervals(cursor)
        for _, d in self.event_intervals.items():
            for k in d:
                in_time, out_time = self.icustays[k[0]][k[1]]
                for start, end in d[k]:
                    start_i = int(math.ceil((in_time-start).total_seconds()/3600))
                    end_i = int(math.ceil((out_time-end).total_seconds()/3600))
                    start_i = min(self.icustay_indices[k][0], start_i)
                    end_i = max(self.icustay_indices[k][1], end_i)
                    self.icustay_indices[k] = (start_i, end_i)

        for _, column in self.event_intervals.items():
            for _, intervals in column.items():
                intervals.sort(key=lambda e: e[1])
                i = 1
                while i < len(intervals):
                    # Make intervals disjoint
                    if intervals[i][0] <= intervals[i-1][1]:
                        intervals[i-1] = (intervals[i-1][0], intervals[i][1])
                        del intervals[i]
                    else:
                        i += 1

        for header, column in self.event_intervals.items():
            for _, intervals in column.items():
                intervals.sort(key=lambda e: e[1])
                for i in range(1, len(intervals)):
                    assert intervals[i][0] > intervals[i-1][1], \
                        "Intervals are disjoint"

    def prepare_event_intervals(self, cursor):
        for drug in DRUGS:
            if 'mv_itemid_test' not in drug:
                # we are forced to skip sodium bicarbonate
                continue
            cursor.execute(("SELECT subject_id, icustay_id, starttime, "
                            "endtime "
                            # change to read from the constructed DRUG_<name>
                            # table to use "big intervals"
                            "FROM inputevents_mv "
                            "WHERE itemid {mv_itemid_test:s} "
                            " AND icustay_id IN (SELECT icustay_id FROM "
                            " selected_patients)").format(**drug))
            for row in cursor:
                key = tuple(row[:2])
                if key in self.icustay_indices:
                    self.event_intervals['B '+drug['drug_name']
                                         ][key].append(tuple(row[2:4]))
        drug_headers = list(self.event_intervals.keys())

        cursor.execute(("SELECT i.subject_id, v.icustay_id, v.starttime, "
                        "v.endtime "
                        "FROM ventdurations v JOIN icustays i "
                        "ON v.icustay_id=i.icustay_id "
                        "WHERE v.icustay_id IN (SELECT icustay_id FROM selected_patients) "))
        for row in cursor:
            key = tuple(row[:2])
            if key in self.icustay_indices:
                self.event_intervals['B in_ventilator'][key].append(tuple(row[2:4]))

        self.headers.append('B pred last_ventilator')
        self.headers.append('F pred hours_until_death')
        self.headers.append('B in_ventilator')
        self.headers += sorted(drug_headers)

        cursor.execute("SELECT subject_id, dod FROM patients")
        self.patient_dod = dict(cursor.fetchall())

    def __iter__(self):
        self.current_hour = None
        self.icustay_iter = iter(sorted(list(self.icustay_indices)))
        self.current_icustay = None
        return self

    def __next__(self):
        if self.current_icustay is None:
            self.current_icustay = next(self.icustay_iter)
        subject_id, icustay_id = self.current_icustay
        start_i, end_i = self.icustay_indices[self.current_icustay]

        while any(map(math.isinf, self.icustay_indices[self.current_icustay])):
            assert all(map(math.isinf, self.icustay_indices[self.current_icustay]))
            for header, d in self.event_intervals.items():
                assert len(d[self.current_icustay]) == 0, \
                    "The ICU stay is empty of events and thus can be discarded"
            self.current_icustay = next(self.icustay_iter)
            subject_id, icustay_id = self.current_icustay
            start_i, end_i = self.icustay_indices[self.current_icustay]

        if self.current_hour is None:
            self.current_hour = start_i
        else:
            self.current_hour += 1
        hour_start = self.icustays[subject_id][icustay_id][0] + \
                     datetime.timedelta(hours=self.current_hour)
        hour_end = hour_start + datetime.timedelta(hours=1)

        r = {'subject_id': subject_id, 'icustay_id': icustay_id,
             'hour': self.current_hour}
        for header, d in self.event_intervals.items():
            interval_list = d[subject_id, icustay_id]
            if self.default_fill is not None:
                r[header] = self.default_fill
            if len(interval_list) > 0:
                if hour_start >= interval_list[0][1]:
                    # Remove the first interval
                    del interval_list[0]
                elif hour_end >= interval_list[0][0]:
                    r[header] = 1

            self.extra_hour_processing(subject_id, r, header, hour_end, interval_list, end_i)
        if self.current_hour >= end_i:
            self.current_hour = self.current_icustay = None
        return r

    def extra_hour_processing(self, subject_id, r, header, hour_end, interval_list, end_i):
        if header == 'B in_ventilator':
            if header in r and r[header] == 1 and (
                    hour_end >= interval_list[0][1] or self.current_hour >= end_i):
                # This is the last hour of the ventilator
                assert len(interval_list) >= 1
                if len(interval_list) == 1:
                    r['B pred last_ventilator'] = 1
                else:
                    r['B pred last_ventilator'] = 0
                if self.patient_dod[subject_id] is None:
                    hours_until_death = 80. * 365*24 # 80 years
                else:
                    hours_until_death = (
                        self.patient_dod[subject_id]-hour_end).total_seconds() / 3600
                r['F pred hours_until_death'] = hours_until_death

class procedureevents_mv(drugevents, HourMixin):
    def prepare_event_intervals(self, cursor):
        family_cluster = {
            228125, # Family meeting held
            228126, # Family met with Case Manager
            228127, # Family met with Social Worker
            228128, # Family updated by MD
            228129, # Family updated by RN
            228136, # Family notified of transfer
            }
            # 228228, "Family meeting attempted, unable"
            # Maybe it's a proxy for family not visiting the patient.
        cursor.execute(("SELECT p.subject_id, p.icustay_id, p.starttime, "
                           "p.endtime, p.itemid, di.label "
                           "FROM procedureevents_mv p JOIN d_items di "
                           "ON di.itemid=p.itemid "
                           "WHERE p.icustay_id IN (SELECT icustay_id FROM selected_patients)"))
        for subject_id, icustay_id, starttime, endtime, itemid, label in cursor:
            key = (subject_id, icustay_id)
            if key in self.icustay_indices:
                if itemid in family_cluster:
                    label = 'Family meeting'
                self.event_intervals['B '+label][key].append((starttime, endtime))
        self.headers += sorted(list(self.event_intervals.keys()))

    def extra_hour_processing(self, *_):
        pass

class labevents(TableIter, HourMixin):
    def item_name(self, id, idx):
        type = self.d_labitems[id]['type']
        if idx is None:
            num = ''
        else:
            num = '{:d} '.format(idx)
            type = type[idx]
        if type == 'bool':
            n = 'F ' # Treat as float
        elif type == 'float':
            n = 'F '
        elif type == 'categorical':
            n = 'C '
        else:
            raise ValueError(type)
        n += num + self.d_labitems[id]['name']
        return n

    def __init__(self, w_cursor, cursor):
        super(labevents, self).__init__(w_cursor, cursor)
        import labevents_clean
        self.d_labitems = labevents_clean.labevents_value_translation
        self.translation = pu.load("translation_labevents.pkl.gz")
        it_cats = list(pu.load("labevents_item_categories.pkl.gz").items())
        it_cats.sort(key=lambda e: -e[1]['frequency'])
        hids = map(lambda t: t[0], it_cats)
        self.hids = {}
        for id in hids:
            if id not in self.d_labitems:
                continue
            item = self.d_labitems[id]
            if not isinstance(item['type'], tuple):
                self.hids[id] = self.item_name(id, None)
                self.headers.append(self.hids[id])
            else:
                for idx in range(len(item['type'])):
                    self.hids[id, idx] = self.item_name(id, idx)
                    self.headers.append(self.hids[id, idx])

        self.c.execute(("SELECT subject_id, NULL as icustay_id, charttime, "
                        "itemid, value, valuenum, valueuom "
                        "FROM labevents l "
                        "WHERE subject_id IN (SELECT subject_id FROM selected_patients) "
                        "ORDER BY subject_id, charttime"))
        self.default_r = {}

    def prepare_last_c_processing(self):
        self.divide_dict = collections.defaultdict(lambda: 0.0, {})
        self.value_dict = collections.defaultdict(lambda: 0.0, {})

    def add_dict(self, key, value, type):
        if value is None:
            return
        if type == 'categorical':
            self.value_dict[key] = value
        elif type in {'bool', 'float'}:
            if type=='bool' and key in self.value_dict and value != self.value_dict[key]:
                print("Bool already in dict", key, value)
            self.value_dict[key] += value
            self.divide_dict[key] += 1
        else:
            raise ValueError

    def process_last_c(self, _):
        subject_id, _, charttime, itemid, value, valuenum, \
            valueuom = self.last_c
        if itemid in self.translation:
            itemid = self.translation[itemid]
        if itemid not in self.d_labitems:
            return
        if valuenum is None and value is not None:
            try:
                value = self.d_labitems[itemid]['values'][value]
            except KeyError:
                try:
                    value = float(value)
                except ValueError:
                    print(itemid, value)
                    import pdb
                    pdb.set_trace()
        else:
            value = valuenum
        if value is None:
            return

        if (not isinstance(value, tuple) and
                isinstance(self.d_labitems[itemid]['type'], tuple)):
            t = list(self.d_labitems[itemid]['type'])
            done = False
            for i, e in enumerate(t):
                if e != 'float' or done:
                    t[i] = 0 # 0 category corresponds to something where there
                             # can be floats
                else:
                    t[i] = value
            value = tuple(t)

        if valueuom is not None:
            valueuom = valueuom.lower()
            if itemid == 50889 and valueuom=="mg/dl":
                value *= 10
            elif itemid == 50916 and valueuom=="ug/dl":
                value *= 10
            elif itemid == 50958 and valueuom=="miu/ml":
                value *= 1000
            elif itemid == 50964 and valueuom=="mosm/kg":
                value *= 1.025
            elif itemid == 50989 and valueuom=="ng/dl":
                value *= 10


        if isinstance(value, tuple):
            for i, v in enumerate(value):
                n = self.hids[itemid, i]
                self.add_dict(n, v, self.d_labitems[itemid]['type'][i])
        else:
            n = self.hids[itemid]
            self.add_dict(n, value, self.d_labitems[itemid]['type'])

    def return_last_c(self, r):
        for i, v in self.value_dict.items():
            if i in self.divide_dict:
                v /= self.divide_dict[i]
            r[i] = v
        return r

TABLES = {'chartevents', 'outputevents', 'drugevents',
            'procedureevents_mv', 'labevents'}
def main():
    if len(sys.argv) != 2 or sys.argv[1] not in TABLES:
        print("Usage: {:s} {:s}".format(sys.argv[0], str(TABLES)))
        sys.exit(1)
    conn_string = "host='localhost' dbname='adria' user='adria' password='adria'"
    conn = psycopg2.connect(conn_string)
    table = sys.argv[1]
    cursor = conn.cursor()
    cursor.execute("SET search_path TO mimiciii")
    iterator = globals()[table](conn.cursor(table), cursor)
    assert len(iterator.headers) == len(set(iterator.headers)), "No duplicate headers"
    with open(table+'.csv', 'wt') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=iterator.headers)
        writer.writeheader()
        i = 1
        print("Started iterating")
        prev_row = {'subject_id': 0, 'icustay_id': 0, 'hour': 0}
        for row in iterator:
            if len(row) == 3:
                continue
            if i%100000 == 0:
                print("Doing row {:d}".format(i))
            writer.writerow(row)
            csvfile.flush()

            assert row['subject_id'] >= 0
            assert row['icustay_id'] >= 0
            if row['icustay_id'] == prev_row['icustay_id']:
                assert row['hour'] >= prev_row['hour'] + 1, \
                    "hour {:d} != prev_hour {:d} + 1".format(row['hour'], prev_row['hour'])
            prev_row = row
            i += 1

if __name__ == '__main__':
    main()
