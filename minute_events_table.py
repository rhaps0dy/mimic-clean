#!/usr/bin/env python3

####
# create the mimic tables, but by minutes and irregular time steps
####

import create_events_table as cet
import pickle
import psycopg2
import collections

class MinuteMixin:
    period_length = 60.
class chartevents(cet.chartevents, MinuteMixin):
    pass
class outputevents(cet.outputevents, MinuteMixin):
    pass
class labevents(cet.labevents, MinuteMixin):
    pass

class IntervalMixin:
    def get_icustay_events(self, subject_id, icustay_id):
        events = dict(map(lambda e: (e, []), self.event_intervals.keys()))
        events['labels'] = []
        for k in list(events.keys()):
            ev_ivs = self.event_intervals[k][subject_id, icustay_id]
            for i, (s, e) in enumerate(ev_ivs):
                si, _, _ = self.hour_i_end(subject_id, icustay_id, s)
                ei, _, _ = self.hour_i_end(subject_id, icustay_id, e)
                if len(events[k]) > 0 and events[k][-1][1] >= si:
                    events[k][-1] = (icustay_id, ei+1, 0)
                else:
                    events[k].append((icustay_id, si, 1))
                    events[k].append((icustay_id, ei+1, 0))
                if k == 'B in_ventilator':
                    last_vent = (i == len(ev_ivs)-1)
                    if self.patient_dod[subject_id] is None:
                        hours_until_death = 80. * 365*24 # 80 years
                    else:
                        hours_until_death = (
                            (self.patient_dod[subject_id]-e)
                            .total_seconds() / 3600.)
                    events['labels'].append((
                        icustay_id, ei, last_vent, hours_until_death))
        return events

class drugevents(cet.drugevents, IntervalMixin, MinuteMixin):
    pass

class procedureevents_mv(cet.procedureevents_mv, IntervalMixin, MinuteMixin):
    pass

SOFT_ROW_LIMIT = int(1e7)

def main():
    conn_string = "host='localhost' dbname='adria' user='adria' password='adria'"
    conn = psycopg2.connect(conn_string)
    d = {}
    for table in cet.TABLES:
        c = conn.cursor()
        c.execute("SET search_path TO mimiciii")
        d[table] = globals()[table](conn.cursor(table), c)
    NON_INTERVALS = ['chartevents', 'outputevents', 'labevents']
    INDICES = {'hour', 'icustay_id', 'subject_id'}
    MINUTE_CUTOFF = -48*60


    ended = {}
    prev_rows = {}
    for table in NON_INTERVALS:
        d[table] = iter(d[table])
        try:
            prev_rows[table] = next(d[table])
            ended[table] = False
        except StopIteration:
            prev_rows[table] = None
            ended[table] = True

    n_rows = 0
    icustay_id = 0
    data = collections.defaultdict(lambda: [], {})
    while not all(v for _, v in ended.items()):
        print("Starting from icustay {:d}".format(icustay_id))
        while n_rows < SOFT_ROW_LIMIT and not all(v for _, v in ended.items()):
            able_tables = list(filter(lambda t: not ended[t], NON_INTERVALS))
            subject_id = min(prev_rows[t]['subject_id'] for t in able_tables)
            icustay_id = None
            for t in able_tables:
                if prev_rows[t]['subject_id'] == subject_id:
                    icu = prev_rows[t]['icustay_id']
                    val = d['chartevents'].icustays[subject_id][icu]
                    if icustay_id is None or val < prev_val:
                        icustay_id = icu
                        prev_val = val
            del icu, val, prev_val

            for table in NON_INTERVALS:
                if ended[table]:
                    continue
                r = prev_rows[table]
                try:
                    while r['icustay_id'] == icustay_id:
                        minute = r['hour']
                        if minute <= MINUTE_CUTOFF:
                            r_ = next(d[table])
                            for h in r_: # Forward fill
                                r[h] = r_[h]
                        else:
                            for header in r:
                                if header not in INDICES and r[header] is not None:
                                    data[header].append((
                                        icustay_id, minute, r[header]))
                                    n_rows += 1
                            r = next(d[table])
                    prev_rows[table] = r
                except StopIteration:
                    ended[table] = True
            for t in 'drugevents', 'procedureevents_mv':
                for header, lst in (
                        d[t].get_icustay_events(subject_id, icustay_id).items()):
                    data[header] += lst
                    n_rows += len(lst)
        with open('no_nan/data_lt_{:d}.pkl'.format(icustay_id), 'wb') as f:
            pickle.dump(dict(data), f)
        del data
        n_rows = 0
        data = collections.defaultdict(lambda: [], {})

if __name__ == '__main__':
    main()
