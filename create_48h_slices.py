#!/usr/bin/env python3

import numpy as np
import pandas as pd
import itertools as it
import gzip
import csv
import math

from pkl_utils import *

def get_headers(table):
    with gzip.open('../mimic-clean/{:s}.csv.gz'.format(table), 'rt',
            newline='') as csvf:
        return next(iter(csv.reader(csvf)))[3:]

def determine_type(header, b_is_category):
    t = header[0]
    if t == 'C':
        return "category"
    elif t == 'B':
        return "category" if b_is_category else np.bool
    elif t == 'F':
        return np.float32
    else:
        raise ValueError(header)

@memoize_pickle('48h.pkl')
def get_frequent_headers():
    zero_headers = get_headers('outputevents')
    bool_headers = (get_headers('procedureevents_mv') +
                    get_headers('drugevents'))
    nan_headers = get_headers('labevents') + get_headers('chartevents')

    dtype = dict(it.chain(
        map(lambda e: (e, determine_type(e, True)), nan_headers + zero_headers),
        map(lambda e: (e, determine_type(e, False)), bool_headers),
        map(lambda e: (e, np.int32), ["icustay_id", "hour", "subject_id"])))
    fillna = dict(it.chain(
        map(lambda e: (e, 0.0), zero_headers),
        map(lambda e: (e, False), bool_headers)))

    headers, _ = load_pickle("non_missing.pkl")
    headers.sort()
    headers = list(filter(lambda t: dtype[t[1]] != "category",
                          headers))
    _, usecols = zip(*headers[-103:])
    usecols = list(usecols)
    usecols.remove('subject_id')
    usecols.append('B pred last_ventilator')
    del fillna['B pred last_ventilator']

    df = pd.read_csv('../mimic.csv.gz', header=0, index_col='icustay_id',
                usecols=usecols, dtype=dtype, engine='c', true_values=[b'1'],
                false_values=[b'0', b''])
    return df.fillna(fillna)

@memoize_pickle('training_examples.pkl')
def get_training_examples(mimic, example_len):
    examples = []
    for icustay_id, df in mimic.groupby(level=0):
        ventilation_ends = df[['hour', 'B pred last_ventilator']].dropna()
        ventilation_ends = iter(ventilation_ends.values)
        try:
            hour, label = next(ventilation_ends)
            features = df.select(lambda h: h not in ['hour',
                                     'B pred last_ventilator'], axis=1)
            hours = df['hour']
            val = np.empty(features.shape[1], dtype=np.float32)
            val[:] = np.nan
            t = np.zeros_like(val, dtype=np.int32)
            t += 999 # features initially missing are assumed very stale
            vent_ts = np.empty((example_len, len(val)), dtype=np.float32)
            vent_ts_i = 0

            for i in range(features.shape[0]):
                f = features.iloc[i,:].values.astype(np.float32)
                n = np.isnan(f)

                account_end = True
                if hours.iloc[i] > hour-example_len:
                    while vent_ts_i < hours.iloc[i] - hour + example_len:
                        if vent_ts_i == 0:
                            if hours.iloc[i] - hour + example_len == 1:
                                account_end = False
                                val[-n] = f[-n]
                                t[-n] = 0
                                t += n
                            vent_ts[vent_ts_i,:] = val
                            vent_ini_t = t.copy()
                        elif vent_ts_i < hours.iloc[i] - hour + example_len - 1:
                            _v = hours.iloc[i] - hour + example_len - 1
                            vent_ts[vent_ts_i:_v,:] = np.nan
                            vent_ts_i = _v - 1
                        else:
                            vent_ts[vent_ts_i,:] = f
                        vent_ts_i += 1
                if hours.iloc[i] == hour:
                    examples.append((icustay_id, hour, label, vent_ini_t, vent_ts.copy()))
                    vent_ts_i = 0
                    hour, label = next(ventilation_ends)
                else:
                    assert hours.iloc[i] < hour

                if account_end:
                    val[-n] = f[-n]
                    t[-n] = 0
                    t += n
        except StopIteration:
            pass
    return examples

if __name__ == '__main__':
    mimic = get_frequent_headers()
    tex = get_training_examples(mimic, 48)
