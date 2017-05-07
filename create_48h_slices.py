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
        hour, label = next(ventilation_ends)
        features = df.select(lambda h: h not in ['hour',
                                 'B pred last_ventilator'], axis=1)
        hours = df['hour']
        val = np.array([features.iloc[0,:].values]*example_len)
        t = np.zeros_like(val[0,:], dtype=np.int32)
        t += np.isnan(val[0,:])
        t *= 1000 # features from long ago are assumed very stale

        if hours.iloc[i] > hour-example_len:
            vent_ini_t = t.copy()
            vent_ini_ffill = ffill.copy()
        if hours.iloc[i] == hour:
            examples.append((icustay_id, hour, label, vent_ini_t, vent_ts))
        else:
            assert hours.iloc[i] < hour

        for i in range(1, features.shape[0]):
            f = features.iloc[i,:].values
            n = np.isnan(f)
            t += n
            t[-n] = 0


if __name__ == '__main__':
    mimic = get_frequent_headers()
    tex = get_training_examples(mimic, 48)
