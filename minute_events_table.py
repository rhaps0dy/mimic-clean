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
class chartevents(MinuteMixin, cet.chartevents):
    pass
class outputevents(MinuteMixin, cet.outputevents):
    pass
class labevents(MinuteMixin, cet.labevents):
    pass

class IntervalMixin:
    def get_icustay_events(self, subject_id, icustay_id):
        events = dict(map(lambda e: (e, []), self.event_intervals.keys()))
        for k in list(events.keys()):
            for s, e in self.event_intervals[k][subject_id, icustay_id]:
                si, _, _ = self.hour_i_end(subject_id, icustay_id, s)
                ei, _, _ = self.hour_i_end(subject_id, icustay_id, e)
                if len(events[k]) > 0 and events[k][-1][1] >= si:
                    events[k][-1] = (icustay_id, ei+1, 0)
                else:
                    events[k].append((icustay_id, si, 1))
                    events[k].append((icustay_id, ei+1, 0))
        return events

class drugevents(cet.drugevents, IntervalMixin, MinuteMixin):
    pass

class procedureevents_mv(cet.procedureevents_mv, IntervalMixin, MinuteMixin):
    pass

SOFT_ROW_LIMIT = 100000

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
        prev_rows[table] = next(d[table])
        ended[table] = False

    n_rows = 0
    r = {'icustay_id': 0}
    data = collections.defaultdict(lambda: [], {})
    while not any(map(lambda t: t[1], ended.items())):
        print("Starting from icustay {:d}".format(r['icustay_id']))
        while n_rows < SOFT_ROW_LIMIT:
            for table in NON_INTERVALS:
                if ended[table]:
                    continue
                r = prev_rows[table]
                try:
                    while r['icustay_id'] == prev_rows[table]['icustay_id']:
                        minute = r['hour']
                        if minute <= MINUTE_CUTOFF:
                            r_ = next(d[table])
                            for h in r_: # Forward fill
                                r[h] = r_[h]
                        else:
                            for header in r:
                                if header not in INDICES and r[header] is not None:
                                    data[header].append((
                                        r['icustay_id'], r['hour'], r[header]))
                                    n_rows += 1
                            r = next(d[table])
                    prev_rows[table] = r
                except StopIteration:
                    ended[table] = True
                    for header in r:
                        if r not in INDICES and r[header] is not None:
                            data[header].append((
                                r['icustay_id'], r['hour'], r[header]))
                            n_rows += 1
            for t in 'drugevents', 'procedureevents_mv':
                for header, lst in (
                        d[t].get_icustay_events(
                            prev_rows['chartevents']['subject_id'], r['icustay_id']).items()):
                    data[header] += lst
                    n_rows += len(lst)
        with open('no_nan/data_lt_{:d}.pkl'.format(
                prev_rows['chartevents']['icustay_id']), 'wb') as f:
            pickle.dump(dict(data), f)
        del data
        n_rows = 0
        data = collections.defaultdict(lambda: [], {})

if __name__ == '__main__':
    main()
